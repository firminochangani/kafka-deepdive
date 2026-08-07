#!/bin/bash

uuids=(
  "e7d4579d-c616-4cde-9811-b915a32d373f"
  "5ebf3e2a-149a-4cad-ad4d-ed4e289b373b"
  "e404d402-a354-4a8a-bebd-a7d50095a404"
  "cb7c3c83-05c9-4d8b-89b7-a8dbbf567b38"
  "d891d758-70cc-40db-9753-5edf805189be"
  "efb10b98-bc8d-4a5e-b41f-b27f91413194"
)

for i in {0..100}
do
  listing_id="${uuids[$((i % 6))]}"
  curl -X POST -H "Content-Type: application/json" -d "{ \"listing_id\": \"$listing_id\" }" http://localhost:8085/listing
  sleep 0.25
done