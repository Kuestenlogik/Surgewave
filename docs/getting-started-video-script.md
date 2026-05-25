# Surgewave — Getting Started in 5 Minutes (Video Script)

## Intro (0:00 - 0:30)

**Voiceover:** "Surgewave is a high-performance event streaming platform built with .NET 10. Think of it as a Kafka replacement — but faster, simpler, and with built-in AI, visual pipelines, and an admin dashboard. Let me show you what makes it special."

**Screen:** Surgewave README hero section, scroll through comparison table

---

## 1. Start the Broker (0:30 - 1:00)

**Voiceover:** "Starting Surgewave takes one command. No ZooKeeper, no Java, no complex config."

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker
```

**Screen:** Terminal showing broker startup with ports (9092, 9093, 1883, 5050)

**Voiceover:** "That's it. Kafka protocol on 9092, gRPC on 9093, MQTT for IoT on 1883, and the Control UI on port 5050."

---

## 2. Dashboard (1:00 - 1:30)

**Voiceover:** "Open localhost:5050 — this is Surgewave Control. A full admin dashboard, not a third-party tool."

**Screen:** Browser opens Surgewave Control Dashboard
- Show 14 widgets (throughput, latency, brokers, topics)
- Toggle dark/light mode
- Drag a widget

**Voiceover:** "14 customizable widgets, dark mode, drag-and-drop layout. Everything persisted in your browser."

---

## 3. Produce & Consume (1:30 - 2:15)

**Voiceover:** "Let's send some messages. Surgewave's Native Client is zero-config."

```csharp
using var client = new SurgewaveNativeClient("localhost", 9092);

// Produce
await client.Messaging.SendAsync("demo-topic", 0, "key"u8, "Hello Surgewave!"u8);

// Consume
var msg = await client.Messaging.ReceiveAsync("demo-topic", 0, 0);
Console.WriteLine(Encoding.UTF8.GetString(msg.Value));
```

**Screen:** Code editor with the above, then terminal output: "Hello Surgewave!"

**Voiceover:** "Or use the existing Confluent.Kafka client — Surgewave is 100% Kafka protocol compatible. Zero code changes for migration."

```csharp
// Works with Confluent.Kafka — no changes needed
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();
await producer.ProduceAsync("demo-topic", new Message<string, string> { Value = "Hello from Kafka client!" });
```

---

## 4. Visual Pipeline Editor (2:15 - 3:00)

**Voiceover:** "This is where Surgewave gets interesting. The Visual Pipeline Editor — drag and drop your data flows."

**Screen:** Open /pipelines, create new pipeline
- Drag a Generator source node
- Drag a Transform node
- Drag a Sink node
- Connect them
- Click Start

**Voiceover:** "6 node categories: Source, Sink, Transform, AI/ML, Workflow, and Streams. Build complex data pipelines without writing code."

**Screen:** Show pipeline running with live data preview

**Voiceover:** "And yes — you can chat with your pipeline. Click the chat button..."

**Screen:** Open Chat drawer, send a message, show streaming response

---

## 5. AI Integration (3:00 - 3:45)

**Voiceover:** "Surgewave has built-in AI. Let me build a RAG pipeline in 30 seconds."

**Screen:** Create from template → "RAG Chatbot"
- Show 7-node pipeline appear
- Configure LLM (OpenAI/Ollama)
- Start pipeline
- Chat with it

**Voiceover:** "RAG, Agents, Guardrails, ONNX ML scoring — all as pipeline nodes. No external services needed."

**Screen:** Show Agent Design Studio (/agents/builder)
- 6 tabs: Persona, Behavior, Tools, Knowledge, Guardrails, Test
- Quick test chat

**Voiceover:** "The Agent Design Studio lets you build AI agents visually — persona, tools, guardrails, test chat — then deploy as a pipeline node."

---

## 6. Surgewave Assistant (3:45 - 4:15)

**Voiceover:** "Surgewave has its own AI ops assistant. Ask it anything about your cluster."

**Screen:** Click brain icon in AppBar, Assistant drawer opens

```
User: "How's my cluster doing?"
Assistant: "Cluster healthy. 3 brokers, 12 topics, throughput 1.2K msg/s.
           No anomalies detected. Recommendation: consider enabling
           compression for topics with >10KB average message size."
```

**Voiceover:** "It analyzes metrics, detects anomalies, suggests tuning — even generates SQL from natural language."

```
User: "Show me the last 10 orders over 100 euros"
Assistant: Generated SQL: SELECT * FROM orders WHERE amount > 100
           ORDER BY _timestamp DESC LIMIT 10
           [Execute] [Open in SQL Editor]
```

---

## 7. What Else? (4:15 - 4:45)

**Voiceover:** "There's so much more:"

**Screen:** Quick montage with text overlays:
- "10 Storage Engines" → show storage index page
- "113 Connectors" → show marketplace
- "WASM Plugins" → show wasm page
- "Schema Registry with AI-assisted evolution"
- "Edge-to-Cloud Sync" → show edge diagram
- "Multi-Tenancy, Privacy/GDPR, Geo-Replication"
- "Serverless Functions, GraphQL, MQTT, WebSocket"
- "Chaos Testing, Performance Benchmarks"

**Voiceover:** "113 connectors, WASM plugins, schema inference, edge deployment, privacy by design, multi-tenancy, and much more."

---

## 8. Get Started (4:45 - 5:00)

**Voiceover:** "Surgewave is open source, .NET 10, single binary. Try it now."

**Screen:** Terminal commands:
```bash
git clone https://github.com/Kuestenlogik/Surgewave
cd Surgewave
dotnet run --project src/Kuestenlogik.Surgewave.Broker
# Open http://localhost:5050
```

**Screen:** Final shot of Surgewave Control dashboard

**Voiceover:** "Surgewave — the AI-native streaming platform for .NET. Star us on GitHub."

**Screen:** GitHub repo with star button highlighted

---

## Production Notes

- **Duration:** ~5 minutes
- **Format:** Screen recording with voiceover
- **Resolution:** 1920x1080
- **Tools:** OBS Studio for recording, DaVinci Resolve for editing
- **Music:** Subtle background track (royalty-free)
- **Captions:** Auto-generated, reviewed
- **Thumbnail:** Surgewave logo + "5 min Getting Started" + dashboard screenshot
