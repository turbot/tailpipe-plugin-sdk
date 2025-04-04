module github.com/turbot/tailpipe-plugin-sdk

go 1.24

//replace github.com/turbot/pipe-fittings/v2 => ../pipe-fittings

require (
	github.com/aws/aws-sdk-go-v2/config v1.27.11
	github.com/dustin/go-humanize v1.0.1
	github.com/elastic/go-grok v0.3.1
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/go-plugin v1.6.1
	github.com/hashicorp/hcl/v2 v2.20.1
	github.com/iancoleman/strcase v0.3.0
	github.com/itchyny/timefmt-go v0.1.6
	github.com/marcboeker/go-duckdb v1.8.4
	github.com/rs/xid v1.5.0
	github.com/satyrius/gonx v1.4.0
	github.com/stretchr/testify v1.10.0
	github.com/turbot/go-kit v1.2.0
	github.com/turbot/pipe-fittings/v2 v2.3.1
	github.com/zclconf/go-cty v1.14.4
	golang.org/x/exp v0.0.0-20250128182459-e0ece0dbea4c
	golang.org/x/sync v0.11.0
	golang.org/x/time v0.5.0
	google.golang.org/grpc v1.69.2
	google.golang.org/protobuf v1.36.1
)
