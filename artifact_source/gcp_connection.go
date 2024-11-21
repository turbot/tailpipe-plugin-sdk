package artifact_source

import (
	"context"
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

type GcpConnection struct {
	Project      *string `json:"project" hcl:"project"`
	Credentials  *string `json:"credentials" hcl:"credentials"`
	QuotaProject *string `json:"quota_project" hcl:"quota_project"`
	Impersonate  *string `json:"impersonate" hcl:"impersonate"`
}

func (c *GcpConnection) Validate() error {
	return nil
}

func (c *GcpConnection) Identifier() string {
	return "gcp"
}

func (c *GcpConnection) GetProject() string {
	// return if set
	if c.Project != nil {
		return *c.Project
	}

	// else check environment variables
	envVars := []string{"CLOUDSDK_CORE_PROJECT", "GCP_PROJECT"}
	for _, envVar := range envVars {
		if val, exists := os.LookupEnv(envVar); exists {
			return val
		}
	}

	// TODO: #connection is there another location to check for an active project?

	return ""
}

func (c *GcpConnection) GetClientOptions(ctx context.Context) ([]option.ClientOption, error) {
	var opts []option.ClientOption

	// credentials
	if c.Credentials != nil {
		contents, err := c.pathOrContents(*c.Credentials)
		if err != nil {
			return opts, fmt.Errorf("error reading credentials file: %v", err)
		}
		opts = append(opts, option.WithCredentialsJSON([]byte(contents)))
	}

	// quota project
	qp := os.Getenv("GOOGLE_CLOUD_QUOTA_PROJECT")
	if c.QuotaProject != nil {
		qp = *c.QuotaProject
	}
	if qp != "" {
		opts = append(opts, option.WithQuotaProject(qp))
	}

	// impersonation of service account
	if c.Impersonate != nil {
		ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
			TargetPrincipal: *c.Impersonate,
			Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform"},
		})
		if err != nil {
			return opts, err
		}

		opts = append(opts, option.WithTokenSource(ts))
	}
	return opts, nil
}

// TODO: Determine where this actually belongs, maybe a useful util func? https://github.com/turbot/tailpipe-plugin-sdk/issues/14
func (c *GcpConnection) pathOrContents(in string) (string, error) {
	if len(in) == 0 {
		return "", nil
	}

	filePath := in

	if filePath[0] == '~' {
		var err error
		filePath, err = homedir.Expand(filePath)
		if err != nil {
			return filePath, err
		}
	}

	if _, err := os.Stat(filePath); err == nil {
		contents, err := os.ReadFile(filePath)
		if err != nil {
			return string(contents), err
		}
		return string(contents), nil
	}

	if len(filePath) > 1 && (filePath[0] == '/' || filePath[0] == '\\') {
		return "", fmt.Errorf("%s: no such file or dir", filePath)
	}

	return in, nil
}
