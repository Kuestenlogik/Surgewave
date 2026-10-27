# Kafka message schemas (test data)

The request and response schemas of every API the broker advertises, copied unchanged from Apache Kafka
(`clients/src/main/resources/common/message/`, commit `46dc4b51f7`, 2026-10-20). They are what Kafka generates its
own codecs from; `Conformance/RequestSchemaConformanceTests` checks the broker's hand-written codecs against them.

Licensed under the Apache License 2.0 (header in each file); see the repository's NOTICE.

To move to a newer Kafka: copy the files again from a Kafka checkout, update the commit above, run the
conformance tests, and fix or deliberately except what they report.
