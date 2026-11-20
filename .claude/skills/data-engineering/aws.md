# AWS wiring and guardrails

## Crawl stage
- EventBridge schedule -> Step Functions -> Lambda (fetch one page) -> S3 `raw/<source>/<yyyy-mm-dd>/<id>.json|html`.
- Queue of URLs in SQS (with DLQ) so failures retry and don't block others. Concurrency limit on the Lambda = politeness limit.
- Lambda: package deps with a layer or container image; timeout and memory set explicitly; env vars for bucket names only.
- Glue Python shell/Spark only if a job exceeds Lambda limits or needs heavy transforms. Say why before choosing it.

## Storage
- S3: block public access, versioning on raw bucket, lifecycle rule to move old raw to Infrequent Access, SSE-S3 or KMS.
- Postgres (RDS/Aurora Serverless v2): private subnet, security group allowing only Lambda/Glue SGs, creds in Secrets Manager. Lambda in a VPC needs a NAT or VPC endpoints to reach the internet/S3 - NAT gateways are a common surprise cost; prefer S3 gateway endpoint and keep the crawler Lambda outside the VPC, writing only to S3, with a separate in-VPC loader Lambda.
- Loader: S3 event -> loader Lambda -> bronze table (`COPY`/batched `INSERT ... ON CONFLICT`).

## IAM
- One role per function. Grant exact actions (`s3:PutObject` on `arn:.../raw/*`), not `*`.
- Show the policy JSON/HCL and explain each statement.

## Cost guardrails
- Create an AWS Budget alarm before anything else.
- Flag always-on costs: NAT gateway, RDS instance hours, Aurora min ACUs, Neptune, CloudWatch log retention (set retention to 14-30 days).
- Tag every resource `project=boiler-room-analytics`.
- Tear-down path must exist (`terraform destroy` / `cdk destroy`) and be mentioned.

## Observability
- Structured JSON logs, CloudWatch alarm on DLQ depth and Lambda errors, a run summary row (pages fetched/failed/loaded) written per execution.
