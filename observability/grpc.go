package observability

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const requestIDMetadataKey = "x-request-id"

// UnaryServerInterceptor logs and correlates unary gRPC requests.
func UnaryServerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		ctx = serverRequestContext(ctx)
		finish := Start(ctx, logger, "grpc", "request", "rpc_method", info.FullMethod, "rpc_kind", "unary")
		defer func() {
			finish(err, "grpc_code", grpcCode(err).String(), "error_kind", grpcErrorKind(err))
		}()
		return handler(ctx, req)
	}
}

// StreamServerInterceptor logs and correlates streaming gRPC requests.
func StreamServerInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		ctx := serverRequestContext(stream.Context())
		finish := Start(ctx, logger, "grpc", "request", "rpc_method", info.FullMethod, "rpc_kind", "stream")
		defer func() {
			finish(err, "grpc_code", grpcCode(err).String(), "error_kind", grpcErrorKind(err))
		}()
		return handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
	}
}

// UnaryClientInterceptor propagates request correlation IDs to unary gRPC calls.
func UnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(outgoingRequestContext(ctx), method, req, reply, cc, opts...)
}

// StreamClientInterceptor propagates request correlation IDs to streaming gRPC calls.
func StreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(outgoingRequestContext(ctx), desc, cc, method, opts...)
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextServerStream) Context() context.Context { return s.ctx }

func serverRequestContext(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(requestIDMetadataKey); len(values) > 0 && validRequestID(values[0]) {
			return WithRequestID(ctx, values[0])
		}
	}
	return EnsureRequestID(ctx)
}

func outgoingRequestContext(ctx context.Context) context.Context {
	ctx = EnsureRequestID(ctx)
	return metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, RequestID(ctx))
}

func grpcErrorKind(err error) string {
	if err == nil {
		return ""
	}
	switch grpcCode(err) {
	case codes.Canceled:
		return "canceled"
	case codes.InvalidArgument:
		return "invalid_argument"
	case codes.DeadlineExceeded:
		return "deadline_exceeded"
	case codes.NotFound:
		return "not_found"
	case codes.AlreadyExists:
		return "already_exists"
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.Aborted:
		return "conflict"
	case codes.Unavailable:
		return "unavailable"
	default:
		return "internal"
	}
}

func grpcCode(err error) codes.Code {
	switch {
	case err == nil:
		return codes.OK
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	default:
		return status.Code(err)
	}
}
