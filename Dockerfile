# syntax=docker/dockerfile:1
# agentnode runs the co-located controller + echo harness + sqlite journal for the pod-resume demo.
# The binary is built on the host (pure Go, CGO_ENABLED=0 — modernc.org/sqlite needs no cgo) by
# hack/demo.sh and copied into a minimal distroless image, so the image build is fast and the
# runtime image is tiny. Runs as root so it can write the journal on the mounted PersistentVolume.
FROM gcr.io/distroless/static-debian12
COPY agentnode /agentnode
ENTRYPOINT ["/agentnode"]
