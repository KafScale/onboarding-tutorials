Great idea. This is a very strong example because it shows Kafka (and KafScale) not as “just messaging”, but as the backbone of an agent workflow.

Below is a clean, minimal demo project you can drop into examples/ as a new exercise, e.g.:

E50_js-agent-kafscale-demo

The goal is illustration, not production completeness.

⸻

Example Project: JavaScript Agent Simulation with Kafka / KafScale

What this demo shows
	•	JavaScript (Node.js) client using the Kafka protocol directly
	•	Standard, file-based configuration
	•	Kafka topics used as agent queues
	•	A simple agent loop:
	•	consume task
	•	build prompt
	•	call LLM (stubbed)
	•	produce response
	•	A human panel (CLI or minimal web UI) that submits tasks and receives responses

This aligns perfectly with:
	•	KafScale’s Kafka compatibility
	•	Stateless, queue-driven agent architectures
	•	Your WritingOS / agent workflows

⸻

High-level Architecture

┌──────────────┐
│ Human Panel  │
│ (CLI / UI)   │
└──────┬───────┘
       │ produce
       ▼
┌──────────────────────┐
│ agent.requests       │
│ (Kafka topic)        │
└──────┬───────────────┘
       │ consume
       ▼
┌──────────────────────┐
│ JS Agent Service     │
│ - loads config       │
│ - builds prompt      │
│ - calls LLM (stub)   │
└──────┬───────────────┘
       │ produce
       ▼
┌──────────────────────┐
│ agent.responses      │
│ (Kafka topic)        │
└──────┬───────────────┘
       │ consume
       ▼
┌──────────────┐
│ Human Panel  │
│ shows result │
└──────────────┘


⸻

Topics Used

Topic	Purpose
agent.requests	Tasks sent from human → agent
agent.internal	(Optional) agent scratchpad / logs
agent.responses	Final responses back to human


⸻

Project Structure

E50_js-agent-kafscale-demo/
├── README.md
├── package.json
├── config/
│   └── agent-config.json
├── src/
│   ├── kafka.js
│   ├── agent.js
│   ├── llm.js
│   ├── panel.js
│   └── types.js
└── scripts/
    ├── produce-task.js
    └── consume-response.js


⸻

Standard Configuration File

config/agent-config.json

{
  "kafka": {
    "clientId": "js-agent-demo",
    "brokers": ["localhost:9092"]
  },
  "topics": {
    "requests": "agent.requests",
    "internal": "agent.internal",
    "responses": "agent.responses"
  },
  "agent": {
    "systemPrompt": "You are a helpful technical writing agent.",
    "model": "stub-llm",
    "temperature": 0.3
  }
}

This is the only place where bootstrap URL and ports live.

⸻

Kafka Helper (shared)

src/kafka.js

import { Kafka } from 'kafkajs'
import fs from 'fs'

const config = JSON.parse(
  fs.readFileSync('./config/agent-config.json')
)

export const kafka = new Kafka(config.kafka)

export const createProducer = () => kafka.producer()
export const createConsumer = (groupId) =>
  kafka.consumer({ groupId })

export const topics = config.topics


⸻

Agent Message Format

src/types.js

export const createAgentRequest = ({
  task,
  spec,
  context,
  correlationId
}) => ({
  correlationId,
  task,
  spec,
  context,
  createdAt: new Date().toISOString()
})


⸻

LLM Stub (replace later)

src/llm.js

export async function callLLM(prompt) {
  // Stubbed on purpose
  return `LLM RESPONSE (stub):
Prompt received:
${prompt.substring(0, 500)}
`
}


⸻

Agent Service

src/agent.js

import { createConsumer, createProducer, topics } from './kafka.js'
import { callLLM } from './llm.js'
import fs from 'fs'

const config = JSON.parse(
  fs.readFileSync('./config/agent-config.json')
)

const consumer = createConsumer('agent-service')
const producer = createProducer()

await consumer.connect()
await producer.connect()

await consumer.subscribe({ topic: topics.requests })

console.log('Agent started, waiting for tasks...')

await consumer.run({
  eachMessage: async ({ message }) => {
    const req = JSON.parse(message.value.toString())

    const prompt = `
SYSTEM:
${config.agent.systemPrompt}

CONTEXT:
${req.context}

TASK:
${req.task}

SPEC:
${req.spec}
`

    const result = await callLLM(prompt)

    await producer.send({
      topic: topics.responses,
      messages: [{
        key: req.correlationId,
        value: JSON.stringify({
          correlationId: req.correlationId,
          result,
          finishedAt: new Date().toISOString()
        })
      }]
    })
  }
})


⸻

Human Panel (CLI)

Submit Task

src/panel.js

import readline from 'readline'
import { createProducer, topics } from './kafka.js'
import { createAgentRequest } from './types.js'
import crypto from 'crypto'

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

const producer = createProducer()
await producer.connect()

rl.question('Task: ', task => {
  rl.question('Spec: ', spec => {
    rl.question('Context: ', async context => {
      const msg = createAgentRequest({
        task,
        spec,
        context,
        correlationId: crypto.randomUUID()
      })

      await producer.send({
        topic: topics.requests,
        messages: [{ value: JSON.stringify(msg) }]
      })

      console.log('Task submitted.')
      rl.close()
      await producer.disconnect()
    })
  })
})


⸻

Read Responses

scripts/consume-response.js

import { createConsumer, topics } from '../src/kafka.js'

const consumer = createConsumer('panel-response')

await consumer.connect()
await consumer.subscribe({ topic: topics.responses })

console.log('Waiting for agent responses...')

await consumer.run({
  eachMessage: async ({ message }) => {
    console.log(
      JSON.parse(message.value.toString())
    )
  }
})


⸻

How to Run the Demo

# install deps
npm install

# terminal 1: start agent
node src/agent.js

# terminal 2: start response listener
node scripts/consume-response.js

# terminal 3: submit task
node src/panel.js


⸻

Why This Example Is Powerful

This demo shows:
	•	Kafka as agent orchestration fabric
	•	Clean separation of:
	•	human input
	•	agent logic
	•	LLM execution
	•	How stateless agents emerge naturally
	•	Why Kafka-compatible systems (like KafScale) are ideal for:
	•	agent swarms
	•	WritingOS
	•	multi-step AI workflows

⸻

Natural Extensions (Later Exercises)
	•	Replace LLM stub with real provider
	•	Add agent.internal scratchpad topic
	•	Add retry / dead-letter topic
	•	Add Web UI instead of CLI
	•	Add multiple agents consuming same request topic

⸻

If you want, next I can:
	•	Turn this into E50 tutorial markdown
	•	Add an architecture diagram
	•	Integrate this with your claims registry
	•	Or adapt it to Semantica-powered agent memory

Just say the word.