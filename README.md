# KafScale Onboarding Tutorials

This repository contains a complete onboarding and tutorial suite for KafScale. It includes a step-by-step developer guide, runnable demos (Java, Spring Boot, Flink, Spark, and Node.js), a claims registry for technical accuracy, and publishing workflows for turning the tutorial into external content.

## Start Here

- Read the core guide: `tutorial/101_kafscale-dev-guide/README.md`
- Follow the chapter flow: `tutorial/101_kafscale-dev-guide/SUMMARY.md`

## What’s Inside

- **Core tutorial**: `tutorial/101_kafscale-dev-guide/`  
  Chapters 1–6 cover local setup, Spring Boot configuration, running the platform demo, troubleshooting, and next steps.
- **Exercises**:
  - `tutorial/E10_java-kafka-client-demo/` — Java client demo (E10)
  - `tutorial/E20_spring-boot-kafscale-demo/` — Spring Boot demo (E20)
  - `tutorial/E30_flink-kafscale-demo/` — Flink word count demo (E30)
  - `tutorial/E40_spark-kafscale-demo/` — Spark word count demo (E40)
  - `tutorial/E50_JS-kafscale-demo/` — KafkaJS agent-simulation demo (E50)
- **Claims registry**: `tutorial/claims/`  
  Single source of truth for architectural, compatibility, and limitation claims used throughout the docs.
- **Publishing workflows**: `tutorial/docuflow/`  
  Process docs for drafting, reviewing, and publishing tutorial content.
- **Agent prompts**: `tutorial/agents/`  
  Specialized prompts for drafting, review, and distribution tasks.
- **Drafts**: `tutorial/publication-drafts/`  
  Prewritten blog/newsletter/LinkedIn/talk drafts based on the tutorial.

## Quick Orientation

If you’re new to KafScale, start with:

1. `tutorial/101_kafscale-dev-guide/01-introduction.md`
2. `tutorial/101_kafscale-dev-guide/02-quick-start.md` (local demo + E10)
3. `tutorial/101_kafscale-dev-guide/03-spring-boot-configuration.md`
4. `tutorial/101_kafscale-dev-guide/04-running-your-app.md` (platform demo + E20)

Optional stream-processing add-ons:

- `tutorial/101_kafscale-dev-guide/07-flink-wordcount-demo.md` (E30)
- `tutorial/101_kafscale-dev-guide/08-spark-wordcount-demo.md` (E40)

## Key Constraints to Keep in Mind

The tutorial is careful to call out limitations from the claims registry, including:

- Kafka transactions and exactly-once semantics are not supported.
- Log compaction is not supported.
- Object storage adds latency compared to local disk.

For the authoritative statements, see:

- `tutorial/claims/README.md`
- `tutorial/claims/limitations.md`
- `tutorial/claims/kafka-compatibility.md`
- `tutorial/claims/kafscale-architecture.md`

## Notes

- The root `README.md` is meant as a directory map; the authoritative instructions live in the tutorial and demo READMEs.
- Each demo folder includes its own prerequisites and run steps.
