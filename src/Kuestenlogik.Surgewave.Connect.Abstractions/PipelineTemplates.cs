namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Provides predefined pipeline templates.
/// </summary>
public static class PipelineTemplates
{
    private static readonly List<PipelineTemplate> _templates =
    [
        new PipelineTemplate
        {
            Id = "file-to-kafka",
            Name = "File to Kafka",
            Description = "Read data from files and stream to a Kafka/Surgewave topic",
            Category = "Data Integration",
            Icon = "folder_open",
            Pipeline = new PipelineExportData
            {
                Name = "File to Kafka Pipeline",
                Description = "Streams file data to a topic",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.FileSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["path"] = "/data/input",
                            ["pattern"] = "*.json",
                            ["poll.interval.ms"] = "1000"
                        },
                        X = 100,
                        Y = 200,
                        Label = "File Source"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.KafkaSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "output-topic",
                            ["bootstrap.servers"] = "localhost:9092"
                        },
                        X = 400,
                        Y = 200,
                        Label = "Kafka Sink"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport
                    {
                        SourceNodeId = "source-1",
                        TargetNodeId = "sink-1"
                    }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "http-webhook",
            Name = "HTTP Webhook Processor",
            Description = "Receive HTTP webhooks and process them through Surgewave",
            Category = "Event Processing",
            Icon = "webhook",
            Pipeline = new PipelineExportData
            {
                Name = "Webhook Processor",
                Description = "Receives and processes HTTP webhooks",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.HttpSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["port"] = "8080",
                            ["path"] = "/webhook"
                        },
                        X = 100,
                        Y = 200,
                        Label = "HTTP Webhook"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.ConsoleSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["format"] = "json"
                        },
                        X = 400,
                        Y = 200,
                        Label = "Console Output"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport
                    {
                        SourceNodeId = "source-1",
                        TargetNodeId = "sink-1"
                    }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "database-sync",
            Name = "Database Change Capture",
            Description = "Capture changes from a database and stream to Surgewave",
            Category = "Data Integration",
            Icon = "storage",
            Pipeline = new PipelineExportData
            {
                Name = "Database CDC Pipeline",
                Description = "Captures database changes via polling",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.SqlServerSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection.string"] = "Server=localhost;Database=mydb;",
                            ["table"] = "orders",
                            ["poll.interval.ms"] = "5000"
                        },
                        X = 100,
                        Y = 200,
                        Label = "SQL Server"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.KafkaSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "cdc-orders",
                            ["bootstrap.servers"] = "localhost:9092"
                        },
                        X = 400,
                        Y = 200,
                        Label = "Surgewave Topic"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport
                    {
                        SourceNodeId = "source-1",
                        TargetNodeId = "sink-1"
                    }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "mqtt-to-kafka",
            Name = "IoT MQTT to Kafka",
            Description = "Bridge IoT sensor data from MQTT to Kafka/Surgewave topics",
            Category = "IoT",
            Icon = "sensors",
            Pipeline = new PipelineExportData
            {
                Name = "MQTT to Kafka Bridge",
                Description = "Streams MQTT messages to Kafka topics",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.MqttSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["broker.url"] = "tcp://localhost:1883",
                            ["topic"] = "sensors/#"
                        },
                        X = 100,
                        Y = 200,
                        Label = "MQTT Source"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.KafkaSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "iot-sensors",
                            ["bootstrap.servers"] = "localhost:9092"
                        },
                        X = 400,
                        Y = 200,
                        Label = "Kafka Sink"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport
                    {
                        SourceNodeId = "source-1",
                        TargetNodeId = "sink-1"
                    }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "s3-data-lake",
            Name = "S3 Data Lake Ingestion",
            Description = "Stream data to S3 for data lake storage",
            Category = "Cloud",
            Icon = "cloud_upload",
            Pipeline = new PipelineExportData
            {
                Name = "S3 Data Lake Pipeline",
                Description = "Ingests data to S3 bucket",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "events",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "s3-ingestion"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Kafka Source"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.S3SinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["bucket"] = "my-data-lake",
                            ["region"] = "us-east-1",
                            ["format"] = "parquet"
                        },
                        X = 400,
                        Y = 200,
                        Label = "S3 Sink"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport
                    {
                        SourceNodeId = "source-1",
                        TargetNodeId = "sink-1"
                    }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "elasticsearch-search",
            Name = "Elasticsearch Indexer",
            Description = "Index data to Elasticsearch for full-text search",
            Category = "Search",
            Icon = "search",
            Pipeline = new PipelineExportData
            {
                Name = "Elasticsearch Indexer Pipeline",
                Description = "Indexes data to Elasticsearch",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "documents",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "es-indexer"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Kafka Source"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.ElasticsearchSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection.url"] = "http://localhost:9200",
                            ["index"] = "documents"
                        },
                        X = 400,
                        Y = 200,
                        Label = "Elasticsearch"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport
                    {
                        SourceNodeId = "source-1",
                        TargetNodeId = "sink-1"
                    }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "rag-pipeline",
            Name = "RAG Embedding Pipeline",
            Description = "Generate embeddings and store in vector database for RAG applications",
            Category = "AI/ML",
            Icon = "psychology",
            Pipeline = new PipelineExportData
            {
                Name = "RAG Embedding Pipeline",
                Description = "Documents → Embeddings → Vector DB",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "documents",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "rag-embedder"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Documents"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "transform-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.AI.OpenAiSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["api.key"] = "${OPENAI_API_KEY}",
                            ["model"] = "text-embedding-3-small",
                            ["mode"] = "embedding"
                        },
                        X = 300,
                        Y = 200,
                        Label = "OpenAI Embeddings"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.AI.QdrantSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["host"] = "localhost",
                            ["port"] = "6334",
                            ["collection"] = "documents"
                        },
                        X = 500,
                        Y = 200,
                        Label = "Qdrant Vector DB"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "transform-1" },
                    new PipelineConnectionExport { SourceNodeId = "transform-1", TargetNodeId = "sink-1" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "event-sourcing",
            Name = "Event Sourcing Pattern",
            Description = "Capture domain events and build projections for CQRS",
            Category = "Architecture",
            Icon = "history",
            Pipeline = new PipelineExportData
            {
                Name = "Event Sourcing Pipeline",
                Description = "Events → Store → Projections",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "domain-events",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "event-projector"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Domain Events"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-postgres",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Database.PostgreSqlSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection.string"] = "Host=localhost;Database=events;",
                            ["table"] = "event_store",
                            ["mode"] = "insert"
                        },
                        X = 400,
                        Y = 100,
                        Label = "Event Store"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-redis",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Redis.RedisSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection"] = "localhost:6379",
                            ["mode"] = "hash",
                            ["key.prefix"] = "projection:"
                        },
                        X = 400,
                        Y = 300,
                        Label = "Read Model Cache"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "sink-postgres" },
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "sink-redis" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "log-analytics",
            Name = "Log Analytics Pipeline",
            Description = "Collect, transform, and analyze application logs",
            Category = "Observability",
            Icon = "analytics",
            Pipeline = new PipelineExportData
            {
                Name = "Log Analytics Pipeline",
                Description = "Logs → Parse → Elasticsearch",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "application-logs",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "log-processor"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Log Stream"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "filter-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Logic.FilterConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["condition"] = "level != 'DEBUG'"
                        },
                        X = 300,
                        Y = 200,
                        Label = "Filter Debug"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Elasticsearch.ElasticsearchSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection.url"] = "http://localhost:9200",
                            ["index.strategy"] = "time",
                            ["index.pattern"] = "logs-{yyyy.MM.dd}"
                        },
                        X = 500,
                        Y = 200,
                        Label = "Elasticsearch"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "filter-1" },
                    new PipelineConnectionExport { SourceNodeId = "filter-1", TargetNodeId = "sink-1" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "microservices-saga",
            Name = "Saga Orchestration",
            Description = "Coordinate distributed transactions across microservices",
            Category = "Architecture",
            Icon = "account_tree",
            Pipeline = new PipelineExportData
            {
                Name = "Order Saga Pipeline",
                Description = "Orchestrate order → inventory → payment flow",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-orders",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "order-requests",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "saga-orchestrator"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Order Requests"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-inventory",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.KafkaSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "inventory-reserve",
                            ["bootstrap.servers"] = "localhost:9092"
                        },
                        X = 350,
                        Y = 100,
                        Label = "Reserve Inventory"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-payment",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.KafkaSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "payment-process",
                            ["bootstrap.servers"] = "localhost:9092"
                        },
                        X = 350,
                        Y = 300,
                        Label = "Process Payment"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-complete",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sinks.KafkaSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "order-completed",
                            ["bootstrap.servers"] = "localhost:9092"
                        },
                        X = 550,
                        Y = 200,
                        Label = "Order Complete"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-orders", TargetNodeId = "sink-inventory" },
                    new PipelineConnectionExport { SourceNodeId = "source-orders", TargetNodeId = "sink-payment" },
                    new PipelineConnectionExport { SourceNodeId = "sink-inventory", TargetNodeId = "sink-complete" },
                    new PipelineConnectionExport { SourceNodeId = "sink-payment", TargetNodeId = "sink-complete" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "real-time-analytics",
            Name = "Real-time Analytics Dashboard",
            Description = "Stream data to InfluxDB for real-time metrics and dashboards",
            Category = "Analytics",
            Icon = "speed",
            Pipeline = new PipelineExportData
            {
                Name = "Real-time Analytics",
                Description = "Metrics → Aggregate → InfluxDB",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "metrics",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "metrics-processor"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Metrics Stream"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "aggregate-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Logic.AggregateConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["window.type"] = "tumbling",
                            ["window.size.ms"] = "60000",
                            ["aggregations"] = "count,sum,avg"
                        },
                        X = 300,
                        Y = 200,
                        Label = "1-min Windows"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.InfluxDb.InfluxDbSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["url"] = "http://localhost:8086",
                            ["bucket"] = "metrics",
                            ["org"] = "surgewave"
                        },
                        X = 500,
                        Y = 200,
                        Label = "InfluxDB"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "aggregate-1" },
                    new PipelineConnectionExport { SourceNodeId = "aggregate-1", TargetNodeId = "sink-1" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "azure-integration",
            Name = "Azure Event Hub to Blob",
            Description = "Stream events from Azure Event Hub to Blob Storage",
            Category = "Cloud",
            Icon = "cloud_sync",
            Pipeline = new PipelineExportData
            {
                Name = "Azure Integration Pipeline",
                Description = "Event Hub → Transform → Blob Storage",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Azure.ServiceBus.ServiceBusSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection.string"] = "${AZURE_SERVICEBUS_CONNECTION}",
                            ["entity.type"] = "queue",
                            ["entity.name"] = "events"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Azure Service Bus"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Azure.Blob.BlobSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection.string"] = "${AZURE_STORAGE_CONNECTION}",
                            ["container"] = "events",
                            ["format"] = "jsonlines",
                            ["partitioner"] = "time"
                        },
                        X = 400,
                        Y = 200,
                        Label = "Azure Blob Storage"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "sink-1" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "postgres-to-bigquery",
            Name = "PostgreSQL CDC to BigQuery",
            Description = "Replicate PostgreSQL changes to BigQuery for analytics",
            Category = "Data Integration",
            Icon = "sync_alt",
            Pipeline = new PipelineExportData
            {
                Name = "PostgreSQL to BigQuery",
                Description = "CDC → Transform → BigQuery",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Database.PostgresCdcSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["connection.string"] = "Host=localhost;Database=app;",
                            ["publication"] = "all_tables",
                            ["slot.name"] = "bq_replication"
                        },
                        X = 100,
                        Y = 200,
                        Label = "PostgreSQL CDC"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "transform-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Logic.MapConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["field.mapping"] = "after.* -> *"
                        },
                        X = 300,
                        Y = 200,
                        Label = "Extract After"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Gcp.BigQuery.BigQuerySinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["project"] = "${GCP_PROJECT}",
                            ["dataset"] = "analytics",
                            ["table"] = "${topic}"
                        },
                        X = 500,
                        Y = 200,
                        Label = "BigQuery"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "transform-1" },
                    new PipelineConnectionExport { SourceNodeId = "transform-1", TargetNodeId = "sink-1" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "slack-alerts",
            Name = "Alert to Slack",
            Description = "Route critical events to Slack channels",
            Category = "Notifications",
            Icon = "notifications_active",
            Pipeline = new PipelineExportData
            {
                Name = "Slack Alerting Pipeline",
                Description = "Events → Filter → Slack",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "source-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Sources.KafkaSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["topic"] = "alerts",
                            ["bootstrap.servers"] = "localhost:9092",
                            ["group.id"] = "slack-alerter"
                        },
                        X = 100,
                        Y = 200,
                        Label = "Alerts"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "filter-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Logic.FilterConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["condition"] = "severity == 'critical' || severity == 'high'"
                        },
                        X = 300,
                        Y = 200,
                        Label = "Filter Critical"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "sink-1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Slack.SlackSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["webhook.url"] = "${SLACK_WEBHOOK_URL}",
                            ["channel"] = "#alerts"
                        },
                        X = 500,
                        Y = 200,
                        Label = "Slack"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "source-1", TargetNodeId = "filter-1" },
                    new PipelineConnectionExport { SourceNodeId = "filter-1", TargetNodeId = "sink-1" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "rag-chatbot",
            Name = "RAG Chatbot",
            Description = "Conversational AI chatbot with retrieval-augmented generation",
            Category = "AI/ML",
            Icon = "smart_toy",
            Pipeline = new PipelineExportData
            {
                Name = "RAG Chatbot Pipeline",
                Description = "ChatEndpoint → DocumentParser → Embedder → Retriever → PromptBuilder → LLM → ChatResponse",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "chat-endpoint",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.ChatEndpointNode",
                        Config = new Dictionary<string, string>
                        {
                            ["port"] = "8080",
                            ["path"] = "/chat"
                        },
                        X = 100,
                        Y = 200,
                        Label = "ChatEndpoint"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "doc-parser",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.DocumentParserNode",
                        Config = new Dictionary<string, string>
                        {
                            ["chunk.size"] = "512",
                            ["chunk.overlap"] = "64"
                        },
                        X = 300,
                        Y = 200,
                        Label = "DocumentParser"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "embedder",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.EmbedderNode",
                        Config = new Dictionary<string, string>
                        {
                            ["api.key"] = "${OPENAI_API_KEY}",
                            ["model"] = "text-embedding-3-small"
                        },
                        X = 500,
                        Y = 200,
                        Label = "Embedder"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "retriever",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.RetrieverNode",
                        Config = new Dictionary<string, string>
                        {
                            ["vector.store.url"] = "http://localhost:6334",
                            ["collection"] = "documents",
                            ["top.k"] = "5"
                        },
                        X = 700,
                        Y = 200,
                        Label = "Retriever"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "prompt-builder",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.PromptBuilderNode",
                        Config = new Dictionary<string, string>
                        {
                            ["template"] = "Answer the question based on the following context:\n{context}\n\nQuestion: {query}",
                            ["context.key"] = "retrieved_docs",
                            ["query.key"] = "user_message"
                        },
                        X = 900,
                        Y = 200,
                        Label = "PromptBuilder"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "llm",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.LlmNode",
                        Config = new Dictionary<string, string>
                        {
                            ["api.key"] = "${OPENAI_API_KEY}",
                            ["model"] = "gpt-4o",
                            ["temperature"] = "0.7",
                            ["max.tokens"] = "2048"
                        },
                        X = 1100,
                        Y = 200,
                        Label = "LLM"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "chat-response",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.ChatResponseNode",
                        Config = new Dictionary<string, string>
                        {
                            ["streaming"] = "true"
                        },
                        X = 1300,
                        Y = 200,
                        Label = "ChatResponse"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "chat-endpoint", TargetNodeId = "doc-parser" },
                    new PipelineConnectionExport { SourceNodeId = "doc-parser", TargetNodeId = "embedder" },
                    new PipelineConnectionExport { SourceNodeId = "embedder", TargetNodeId = "retriever" },
                    new PipelineConnectionExport { SourceNodeId = "retriever", TargetNodeId = "prompt-builder" },
                    new PipelineConnectionExport { SourceNodeId = "prompt-builder", TargetNodeId = "llm" },
                    new PipelineConnectionExport { SourceNodeId = "llm", TargetNodeId = "chat-response" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "document-indexer",
            Name = "Document Indexer",
            Description = "Parse, chunk, embed and index documents into a vector store",
            Category = "AI/ML",
            Icon = "description",
            Pipeline = new PipelineExportData
            {
                Name = "Document Indexer Pipeline",
                Description = "FileSource → DocumentParser → Embedder → VectorStoreSink",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "file-source",
                        ConnectorType = "Kuestenlogik.Surgewave.Connector.FileStream.FileStreamSourceConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["path"] = "/data/documents",
                            ["pattern"] = "*.pdf,*.txt,*.md",
                            ["poll.interval.ms"] = "5000"
                        },
                        X = 100,
                        Y = 200,
                        Label = "FileSource"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "doc-parser",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.DocumentParserNode",
                        Config = new Dictionary<string, string>
                        {
                            ["chunk.size"] = "512",
                            ["chunk.overlap"] = "64"
                        },
                        X = 350,
                        Y = 200,
                        Label = "DocumentParser"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "embedder",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.EmbedderNode",
                        Config = new Dictionary<string, string>
                        {
                            ["api.key"] = "${OPENAI_API_KEY}",
                            ["model"] = "text-embedding-3-small"
                        },
                        X = 600,
                        Y = 200,
                        Label = "Embedder"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "vector-store",
                        ConnectorType = "Kuestenlogik.Surgewave.Connector.VectorStore.VectorStoreSinkConnector",
                        Config = new Dictionary<string, string>
                        {
                            ["host"] = "localhost",
                            ["port"] = "6334",
                            ["collection"] = "documents"
                        },
                        X = 850,
                        Y = 200,
                        Label = "VectorStoreSink"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "file-source", TargetNodeId = "doc-parser" },
                    new PipelineConnectionExport { SourceNodeId = "doc-parser", TargetNodeId = "embedder" },
                    new PipelineConnectionExport { SourceNodeId = "embedder", TargetNodeId = "vector-store" }
                ]
            }
        },
        new PipelineTemplate
        {
            Id = "agent-with-tools",
            Name = "Agent with Tools",
            Description = "Autonomous AI agent with tool access via MCP servers",
            Category = "AI/ML",
            Icon = "psychology",
            Pipeline = new PipelineExportData
            {
                Name = "Agent with Tools Pipeline",
                Description = "ChatEndpoint → Agent → ChatResponse",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "chat-endpoint",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.ChatEndpointNode",
                        Config = new Dictionary<string, string>
                        {
                            ["port"] = "8080",
                            ["path"] = "/agent"
                        },
                        X = 100,
                        Y = 200,
                        Label = "ChatEndpoint"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "agent",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.AgentNode",
                        Config = new Dictionary<string, string>
                        {
                            ["api.key"] = "${OPENAI_API_KEY}",
                            ["model"] = "gpt-4o",
                            ["max.iterations"] = "10",
                            ["mcp.servers"] = "http://localhost:3000",
                            ["system.prompt"] = "You are a helpful AI assistant with access to tools. Use them to answer user questions."
                        },
                        X = 400,
                        Y = 200,
                        Label = "Agent"
                    },
                    new PipelineNodeExport
                    {
                        NodeId = "chat-response",
                        ConnectorType = "Kuestenlogik.Surgewave.AI.Nodes.ChatResponseNode",
                        Config = new Dictionary<string, string>
                        {
                            ["streaming"] = "true"
                        },
                        X = 700,
                        Y = 200,
                        Label = "ChatResponse"
                    }
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "chat-endpoint", TargetNodeId = "agent" },
                    new PipelineConnectionExport { SourceNodeId = "agent", TargetNodeId = "chat-response" }
                ]
            }
        }
    ];

    /// <summary>
    /// All available templates.
    /// </summary>
    public static IReadOnlyList<PipelineTemplate> All => _templates;

    /// <summary>
    /// Get a template by ID.
    /// </summary>
    public static PipelineTemplate? GetById(string id) =>
        _templates.FirstOrDefault(t => t.Id.Equals(id, StringComparison.OrdinalIgnoreCase));
}
