data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  # S3 bucket names are global; the account id keeps them unique and predictable.
  lake_layers = toset(["raw", "silver", "gold"])
  bucket_name = { for layer in local.lake_layers : layer => "${var.project}-${local.account_id}-${layer}" }
}

# ----------------------------------------------------------------------------- data lake
resource "aws_s3_bucket" "lake" {
  for_each      = local.lake_layers
  bucket        = local.bucket_name[each.key]
  force_destroy = true # practice project: `terraform destroy` must be able to clean up
}

resource "aws_s3_bucket_public_access_block" "lake" {
  for_each                = aws_s3_bucket.lake
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  for_each = aws_s3_bucket.lake
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  for_each = aws_s3_bucket.lake
  bucket   = each.value.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# ----------------------------------------------------------------------------- athena
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project}-${local.account_id}-athena-results"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}

    expiration {
      days = 7
    }
  }
}

resource "aws_athena_workgroup" "olist" {
  name          = var.project
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# ----------------------------------------------------------------------------- glue catalog
resource "aws_glue_catalog_database" "silver" {
  name        = "${var.project}_silver"
  description = "Cleaned, typed Olist tables (Parquet on S3)."
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${var.project}_gold"
  description = "Analytics marts (star schema) built by dbt."
}

# ----------------------------------------------------------------------------- iam
# Least-privilege identity for the pipeline running on a laptop/CI.
# No access key is created here on purpose: keys in Terraform state are a leak risk.
# Create one in the console/CLI and store it in ~/.aws/credentials.
data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListLakeBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = concat([for b in aws_s3_bucket.lake : b.arn], [aws_s3_bucket.athena_results.arn])
  }

  statement {
    sid       = "ReadWriteLakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = concat([for b in aws_s3_bucket.lake : "${b.arn}/*"], ["${aws_s3_bucket.athena_results.arn}/*"])
  }

  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition", "glue:CreatePartition",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${local.account_id}:catalog",
      aws_glue_catalog_database.silver.arn,
      aws_glue_catalog_database.gold.arn,
      "arn:aws:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.silver.name}/*",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.gold.name}/*",
    ]
  }

  statement {
    sid = "AthenaQueries"
    actions = [
      "athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution",
      "athena:GetQueryResults", "athena:GetWorkGroup", "athena:BatchGetQueryExecution",
    ]
    resources = [aws_athena_workgroup.olist.arn]
  }
}

resource "aws_iam_policy" "pipeline" {
  name   = "${var.project}-pipeline"
  policy = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_user" "pipeline" {
  name = "${var.project}-pipeline"
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# ----------------------------------------------------------------------------- cost guard
resource "aws_budgets_budget" "monthly" {
  name         = "${var.project}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.budget_email]
    }
  }
}
