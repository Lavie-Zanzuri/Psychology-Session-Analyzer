# Psychology Session Analyzer

An event-driven **microservices** system that analyzes recorded therapy sessions and produces insights for the therapist. A video goes in, and the system extracts the audio, transcribes it with speaker diarization, and uses an LLM to tag what the patient talks about and how they feel about it.

Each service runs in its own **Docker** container. The whole system runs with **Docker Compose**, and the services communicate only through **RabbitMQ** queues, never by calling each other directly.

## Architecture

```
          POST /upload
               │
       ┌───────▼────────┐     video     ┌────────┐
       │ upload-service │ ────────────► │ MinIO  │
       │   (FastAPI)    │               └────────┘
       └───────┬────────┘                    ▲
               │ event (RabbitMQ)             │ mp3
       ┌───────▼─────────┐                    │
       │ audio-extractor │ ───────────────────┘
       │    (ffmpeg)     │
       └───────┬─────────┘
               │ event
       ┌───────▼───────────────┐
       │ transcription-service │  AssemblyAI + speaker diarization
       └───────┬───────────────┘
               │ event
       ┌───────▼──────────┐   cache   ┌───────┐
       │ analysis-service │ ◄───────► │ Redis │
       │ (OpenAI GPT-4o)  │           └───────┘
       └───────┬──────────┘
               │ results
          ┌────▼────┐        ┌─────────────┐
          │ MongoDB │ ◄───── │ api-service │ ◄── GET /videos
          └─────────┘        │  (FastAPI)  │
                             └─────────────┘

All services ship their logs to DataDog through the DataDog Agent.
```

## Services

| Service | Responsibility |
|---|---|
| **upload-service** | FastAPI endpoint for uploading a session video. Stores it in MinIO and publishes an event. |
| **audio-extractor** | Extracts the audio track with ffmpeg, converts it to mp3 and stores it in MinIO. |
| **transcription-service** | Transcribes the audio with AssemblyAI, including speaker diarization. |
| **analysis-service** | Uses an LLM to analyze the transcript and saves the results to MongoDB. LLM calls are cached in Redis. |
| **api-service** | FastAPI service for listing analyzed sessions and getting the analysis of a session. |
| **event-consumer** | A simple consumer used to verify the RabbitMQ integration. |

## What the analysis produces

1. **Speaker identification:** which speaker is the therapist and which is the patient.
2. **Emotion and topic tagging:** each utterance gets an emotion and a topic, for example *"mother"* and *"happy"*.
3. **Positive vs. negative topics:** which topics make the patient feel good or bad, with recommendations for the next session.
4. **Recurring patterns:** themes that come up repeatedly during the session.

## Tech stack

Python · FastAPI · Docker · Docker Compose · RabbitMQ · MinIO · Redis · MongoDB · AssemblyAI · OpenAI API · ffmpeg · DataDog

## Getting started

Requirements: Docker and Docker Compose.

1. Create a `.env` file in the project root (it is ignored by git):

   ```
   OPENAI_API_KEY=your_key
   ASSEMBLYAI_API_KEY=your_key
   DD_API_KEY=your_datadog_key
   DD_SITE=us5.datadoghq.com
   ```

2. Start the whole system:

   ```bash
   docker compose up --build
   ```

3. Upload a session video:

   ```bash
   curl -X POST -F "file=@session.mp4" http://localhost:8000/upload
   ```

4. Get the results:

   ```bash
   curl http://localhost:8001/videos
   curl http://localhost:8001/videos/<video_id>/analysis
   ```

## Ports

| Service | Port |
|---|---|
| upload-service | 8000 |
| api-service | 8001 |
| MinIO API / Console | 9000 / 9001 |
| RabbitMQ / Management UI | 5672 / 15672 |
| Redis | 6379 |
| MongoDB | 27017 |
