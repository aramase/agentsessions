module github.com/aramase/agentsessions/integrations/substrate

go 1.26.3

require (
	github.com/agent-substrate/substrate v0.0.0-20260725015935-aa1d14a7b33b
	github.com/aramase/agentsessions v0.0.0
	google.golang.org/grpc v1.82.1
)

require (
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af // indirect
)

replace github.com/aramase/agentsessions => ../..
