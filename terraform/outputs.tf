output "lake_raw_uri" {
  value = "s3a://${aws_s3_bucket.lake["raw"].bucket}"
}

output "lake_silver_uri" {
  value = "s3a://${aws_s3_bucket.lake["silver"].bucket}"
}

output "lake_gold_uri" {
  value = "s3a://${aws_s3_bucket.lake["gold"].bucket}"
}

output "athena_workgroup" {
  value = aws_athena_workgroup.olist.name
}

output "glue_database_silver" {
  value = aws_glue_catalog_database.silver.name
}

output "glue_database_gold" {
  value = aws_glue_catalog_database.gold.name
}

output "pipeline_iam_user" {
  value = aws_iam_user.pipeline.name
}
