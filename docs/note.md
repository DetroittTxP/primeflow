i provided ui with visualize and animation from prefect and https://temporal.io/ please change ui to align by both products. implement missing appropreate features which it should have. Reliable, scalable, serverless.

implement all pending components and serivces.

NATS transport. The bus.Bus interface is there and Redis implements it; a NATS implementation is a single file, not yet written.
OTEL, Prometheus metrics. Events and the API cover observability today; the HPA example in the manifests assumes a primeflow_queue_ready metric that needs an exporter.
Log retention. pf_logs grows without bound. Add a partition or a cleanup job before production.
Sub-flows. RunDeployment fans out but does not wait for children.
RBAC. A single bearer token, not per-user roles.

at workers tab. i need all setting of worker what is trigger for sprawn new worker, admin can setup min workers and autoscale depends on workload. External dev can have own set of worker. all workers must not lock each other. deadlock prevention is solid.

please review above taks and suggest me for improvement and all missing things as you are Father of Dev. dont' forget to update readme in github.