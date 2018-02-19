package nozzleconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var (
	DefaultWorkers int = 4
)

type NozzleConfig struct {
	UAAURL                  string
	Client                  string
	ClientSecret            string
	TrafficControllerURL    string
	DopplerEndpoint         string
	FirehoseSubscriptionID  string
	DataDogBaseURL          string
	DataDogInputURL         string
	DataDogSeriesURL        string
	DataDogURL              string
	DataDogAPIKey           string
	HTTPProxyURL            string
	HTTPSProxyURL           string
	NoProxy                 []string
	CloudControllerEndpoint string
	DataDogTimeoutSeconds   uint32
	FlushDurationSeconds    uint32
	FlushMaxBytes           uint32
	InsecureSSLSkipVerify   bool
	MetricPrefix            string
	Deployment              string
	DeploymentFilter        string
	DisableAccessControl    bool
	IdleTimeoutSeconds      uint32
	AppMetrics              bool
	NumWorkers              int
	GrabInterval            int
	CustomTags              []string
	DBPath                  string
}

func Parse(configPath string) (*NozzleConfig, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	var config NozzleConfig
	if err != nil {
		return nil, fmt.Errorf("Can not read config file [%s]: %s", configPath, err)
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("Can not parse config file %s: %s", configPath, err)
	}

	overrideWithEnvVar("NOZZLE_UAAURL", &config.UAAURL)
	overrideWithEnvVar("NOZZLE_CLIENT", &config.Client)
	overrideWithEnvVar("NOZZLE_CLIENT_SECRET", &config.ClientSecret)
	overrideWithEnvVar("NOZZLE_TRAFFICCONTROLLERURL", &config.TrafficControllerURL)
	overrideWithEnvVar("NOZZLE_FIREHOSESUBSCRIPTIONID", &config.FirehoseSubscriptionID)
	overrideWithEnvVar("NOZZLE_DATADOGURL", &config.DataDogInputURL)
	overrideWithEnvVar("NOZZLE_DATADOGAPIKEY", &config.DataDogAPIKey)
	overrideWithEnvVar("HTTP_PROXY", &config.HTTPProxyURL)
	overrideWithEnvVar("HTTPS_PROXY", &config.HTTPSProxyURL)
	overrideWithEnvUint32("NOZZLE_DATADOGTIMEOUTSECONDS", &config.DataDogTimeoutSeconds)
	overrideWithEnvVar("NOZZLE_METRICPREFIX", &config.MetricPrefix)
	overrideWithEnvVar("NOZZLE_DEPLOYMENT", &config.Deployment)
	overrideWithEnvVar("NOZZLE_DEPLOYMENT_FILTER", &config.DeploymentFilter)

	overrideWithEnvUint32("NOZZLE_FLUSHDURATIONSECONDS", &config.FlushDurationSeconds)
	overrideWithEnvUint32("NOZZLE_FLUSHMAXBYTES", &config.FlushMaxBytes)

	overrideWithEnvBool("NOZZLE_INSECURESSLSKIPVERIFY", &config.InsecureSSLSkipVerify)
	overrideWithEnvBool("NOZZLE_DISABLEACCESSCONTROL", &config.DisableAccessControl)
	overrideWithEnvUint32("NOZZLE_IDLETIMEOUTSECONDS", &config.IdleTimeoutSeconds)
	overrideWithEnvSliceStrings("NO_PROXY", &config.NoProxy)
	overrideWithEnvVar("NOZZLE_DB_PATH", &config.DBPath)

	if config.MetricPrefix == "" {
		config.MetricPrefix = "cloudfoundry.nozzle."
	}

	if config.NumWorkers == 0 {
		config.NumWorkers = DefaultWorkers
	}

	overrideWithEnvInt("NOZZLE_NUM_WORKERS", &config.NumWorkers)

	var datadogBaseURL string
	var DataDogSeriesURL string

	datadogBaseURL, err = getDatadogAPIURL(config.DataDogInputURL)
	if err != nil {
		return &config, err
	}
	config.DataDogBaseURL = datadogBaseURL
	config.DataDogURL = datadogBaseURL
	DataDogSeriesURL, err = getDatadogSeriesURL(datadogBaseURL)
	if err != nil {
		return &config, err
	}
	config.DataDogSeriesURL = DataDogSeriesURL

	return &config, nil
}

func overrideWithEnvVar(name string, value *string) {
	envValue := os.Getenv(name)
	if envValue != "" {
		*value = envValue
	}
}

func overrideWithEnvUint32(name string, value *uint32) {
	envValue := os.Getenv(name)
	if envValue != "" {
		tmpValue, err := strconv.Atoi(envValue)
		if err != nil {
			panic(err)
		}
		*value = uint32(tmpValue)
	}
}

func overrideWithEnvInt(name string, value *int) {
	envValue := os.Getenv(name)
	if envValue != "" {
		tmpValue, err := strconv.Atoi(envValue)
		if err != nil {
			panic(err)
		}
		*value = int(tmpValue)
	}
}

func overrideWithEnvBool(name string, value *bool) {
	envValue := os.Getenv(name)
	if envValue != "" {
		var err error
		*value, err = strconv.ParseBool(envValue)
		if err != nil {
			panic(err)
		}
	}
}

func overrideWithEnvSliceStrings(name string, value *[]string) {
	envValue := os.Getenv(name)
	*value = strings.Split(envValue, ",")
}

func getDatadogAPIURL(apiurl string) (string, error) {
	parsedURL, err := url.Parse(apiurl)
	if err != nil {
		return "", fmt.Errorf("Error parsing API URL: %v", err)
	}
	if parsedURL.EscapedPath() == "" {
		return parsedURL.String(), nil
	} else {
		return fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Hostname()), nil
	}
}

func getDatadogSeriesURL(datadogAPIURL string) (string, error) {
	parsedURL, err := url.Parse(datadogAPIURL)
	// This should always be passed a proper URL. It's very unlikely this could ever happen.
	if err != nil {
		return "", fmt.Errorf("Error parsing API URL: %v", err)
	}
	parsedURL.Path = "/api/v1/series"
	return parsedURL.String(), nil
}
