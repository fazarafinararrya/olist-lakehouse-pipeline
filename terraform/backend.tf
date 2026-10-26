# Remote state: both machines read and write the same state file, and S3 versioning
# makes a corrupted or accidentally deleted state recoverable.
# This bucket is created outside Terraform (chicken and egg).
terraform {
  backend "s3" {
    bucket       = "olist-tfstate-347288885419"
    key          = "olist/terraform.tfstate"
    region       = "ap-southeast-1"
    encrypt      = true
    use_lockfile = true
  }
}