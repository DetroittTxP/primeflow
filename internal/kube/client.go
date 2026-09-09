// Package kube is the little of the Kubernetes API PrimeFlow needs in order to
// run a flow in a Job: create one, watch it settle, delete it.
//
// It speaks the REST API directly rather than through client-go, which would
// add tens of megabytes of module graph — and a second way of building
// manifests — to a project whose point is one static binary. internal/gitsync
// renders YAML by hand for the same reason.
//
// Nothing here is generic: it knows about batch/v1 Jobs in one namespace, with
// the permissions a launcher's Role grants and no more.
package kube

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// The service-account mount every pod gets. The token is projected and rotates,
// so it is read per request rather than kept.
const (
	saDir       = "/var/run/secrets/kubernetes.io/serviceaccount"
	tokenFile   = saDir + "/token"
	caFile      = saDir + "/ca.crt"
	nsFile      = saDir + "/namespace"
	defaultTTL  = 15 * time.Second
	fieldMgrTag = "primeflow"
)

// Config addresses one cluster. InCluster fills it from the pod's own identity;
// the fields are exported so a test — or an operator debugging from outside —
// can point it somewhere else.
type Config struct {
	// APIServer is the base URL, e.g. https://10.96.0.1:443.
	APIServer string
	// TokenFile is read on every request, because a projected token rotates.
	// Token is the alternative for a caller that has one in hand.
	TokenFile string
	Token     string
	// CAFile is the cluster's CA. Empty means the system pool, which is what a
	// plain http:// APIServer (a test) wants.
	CAFile    string
	Namespace string
	Timeout   time.Duration
}

// InCluster reads the identity kubelet mounts into every pod.
func InCluster() (Config, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return Config{}, errors.New("kube: not running in a cluster (no KUBERNETES_SERVICE_HOST)")
	}
	ns, err := os.ReadFile(nsFile)
	if err != nil {
		return Config{}, fmt.Errorf("kube: read namespace: %w", err)
	}
	return Config{
		APIServer: "https://" + hostPort(host, port),
		TokenFile: tokenFile,
		CAFile:    caFile,
		Namespace: strings.TrimSpace(string(ns)),
	}, nil
}

// hostPort joins a host and port, bracketing an IPv6 literal.
func hostPort(host, port string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}

// Client talks to one cluster's Job API.
type Client struct {
	cfg  Config
	http *http.Client
}

// New builds a client. It fails here, not on the first Job, when the CA cannot
// be read: a launcher that cannot reach its cluster should not start.
func New(cfg Config) (*Client, error) {
	if cfg.APIServer == "" {
		return nil, errors.New("kube: no API server address")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("kube: no namespace")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTTL
	}
	tr := &http.Transport{}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kube: read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kube: %s holds no certificate", cfg.CAFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &Client{cfg: cfg, http: &http.Client{Transport: tr, Timeout: cfg.Timeout}}, nil
}

// Namespace is where this client creates Jobs.
func (c *Client) Namespace() string { return c.cfg.Namespace }

func (c *Client) token() (string, error) {
	if c.cfg.TokenFile == "" {
		return c.cfg.Token, nil
	}
	b, err := os.ReadFile(c.cfg.TokenFile)
	if err != nil {
		return "", fmt.Errorf("kube: read token: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

func (c *Client) jobsURL() string {
	return fmt.Sprintf("%s/apis/batch/v1/namespaces/%s/jobs",
		strings.TrimRight(c.cfg.APIServer, "/"), c.cfg.Namespace)
}

// do sends one request and decodes either the result or the API's own error.
func (c *Client) do(ctx context.Context, method, url string, body []byte, out any) error {
	tok, err := c.token()
	if err != nil {
		return err
	}
	var rdr *bytes.Reader
	if body == nil {
		rdr = bytes.NewReader(nil)
	} else {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return err
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	if resp.StatusCode/100 != 2 {
		var st status
		_ = dec.Decode(&st)
		return &APIError{Code: resp.StatusCode, Reason: st.Reason, Message: st.Message}
	}
	if out == nil {
		return nil
	}
	return dec.Decode(out)
}

// status is the API's error envelope.
type status struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// APIError is a non-2xx answer, kept structured so a caller can tell "already
// exists" and "forbidden" apart without matching on prose.
type APIError struct {
	Code    int
	Reason  string
	Message string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("kubernetes %d %s: %s", e.Code, e.Reason, e.Message)
	}
	return fmt.Sprintf("kubernetes %d %s", e.Code, e.Reason)
}

// NotFound reports whether err is the API saying the object is gone, which is a
// success for a delete and a lost Job for a poll.
func NotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == http.StatusNotFound
}

// Job is the part of a Job's status a launcher acts on.
type Job struct {
	Metadata struct {
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"metadata"`
	Status struct {
		Active     int `json:"active"`
		Succeeded  int `json:"succeeded"`
		Failed     int `json:"failed"`
		Conditions []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"conditions"`
	} `json:"status"`
}

// Finished reports whether the Job has settled, and how. A Job PrimeFlow
// creates has backoffLimit 0 and one pod, so this is that pod's outcome.
func (j *Job) Finished() (done bool, ok bool, reason string) {
	for _, c := range j.Status.Conditions {
		if c.Status != "True" {
			continue
		}
		switch c.Type {
		case "Complete":
			return true, true, ""
		case "Failed":
			return true, false, strings.TrimSpace(c.Reason + " " + c.Message)
		}
	}
	// Older API servers settle the counters before the condition appears.
	switch {
	case j.Status.Succeeded > 0:
		return true, true, ""
	case j.Status.Failed > 0:
		return true, false, "pod failed"
	}
	return false, false, ""
}

// JobList is a page of Jobs, with the labels a sweep needs to attribute them.
type JobList struct {
	Items []struct {
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	} `json:"items"`
}

// ListJobs returns the Jobs matching a label selector.
func (c *Client) ListJobs(ctx context.Context, selector string) (*JobList, error) {
	var list JobList
	if err := c.do(ctx, http.MethodGet, c.jobsURL()+"?labelSelector="+url.QueryEscape(selector), nil, &list); err != nil {
		return nil, err
	}
	return &list, nil
}

// CreateJob posts one Job. The body is the whole object, rendered by Template.
func (c *Client) CreateJob(ctx context.Context, obj map[string]any) (*Job, error) {
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	var job Job
	if err := c.do(ctx, http.MethodPost, c.jobsURL()+"?fieldManager="+fieldMgrTag, body, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// GetJob reads one Job's status.
func (c *Client) GetJob(ctx context.Context, name string) (*Job, error) {
	var job Job
	if err := c.do(ctx, http.MethodGet, c.jobsURL()+"/"+name, nil, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// DeleteJob removes a Job and, with it, the pod running the flow — which is how
// a cancellation reaches a run. Background propagation returns as soon as the
// deletion is recorded; the pod gets its termination grace period to settle the
// run before the kubelet kills it.
func (c *Client) DeleteJob(ctx context.Context, name string) error {
	body, _ := json.Marshal(map[string]any{
		"apiVersion":        "meta/v1",
		"kind":              "DeleteOptions",
		"propagationPolicy": "Background",
	})
	err := c.do(ctx, http.MethodDelete, c.jobsURL()+"/"+name, body, nil)
	if NotFound(err) {
		return nil
	}
	return err
}
