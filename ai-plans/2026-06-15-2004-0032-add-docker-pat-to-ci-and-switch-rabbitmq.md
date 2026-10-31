# Add Docker PAT to CI and Switch RabbitMQ Image

## Rationale

The CI pipeline intermittently fails while pulling RabbitMQ from `public.ecr.aws`, which appears to be caused by registry rate limiting. Move RabbitMQ test pulls back to Docker Hub, authenticate CI with the available Docker Hub credentials, and update the test broker image to the latest RabbitMQ 4.x management image.

## Acceptance Criteria

- [x] CI authenticates to Docker Hub before any test step can pull RabbitMQ images, using the `DOCKERHUB_USER` and `DOCKERHUB_PAT` repository secrets.
- [x] RabbitMQ test infrastructure no longer references `public.ecr.aws/docker/library/rabbitmq`.
- [x] RabbitMQ test infrastructure uses the latest appropriate RabbitMQ 4.x Docker Hub management image, preferring the Alpine variant when it is available.
- [x] The CI workflow keeps working for pull requests and pushes without exposing Docker Hub credentials in logs.
- [x] Automated tests need to be written or updated if the image selection or workflow changes affect covered behavior.

## Technical Details

Update `.github/workflows/ci.yml` to log in to Docker Hub after Docker availability is verified and before restore/build/test can trigger container pulls. Use `docker/login-action` with the repository secrets `${{ secrets.DOCKERHUB_USER }}` and `${{ secrets.DOCKERHUB_PAT }}` rather than an inline `docker login` call.

Update the RabbitMQ Docker image constant in `tests/Transports/Usf.Transport.RabbitMq.Tests/TestSupport/DockerImages.cs` to use Docker Hub's official RabbitMQ image instead of the ECR mirror. Select the latest suitable 4.x management tag available at implementation time, preferring an Alpine management tag if Docker Hub publishes one. Keep the image tag explicit rather than using floating `latest` so local and CI integration tests run against the same broker version.

Verify the workflow syntax and run the RabbitMQ transport test project if practical. If the implementation only changes CI YAML and the Docker image constant, no product-code tests should need new coverage beyond ensuring the existing integration tests still pull and start the broker.
