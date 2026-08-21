package resticstore

func testLocalProvider(root string) Provider {
	return Provider{Repository: &RepositoryProvider{
		Backend: "local", Root: root, Options: map[string]string{"no_preallocate": "false"},
	}}
}

func testS3Provider(endpoint string, useHTTP bool, bucket, prefix, region, accessKey, secretKey string) Provider {
	scheme := "https://"
	if useHTTP {
		scheme = "http://"
	}
	return Provider{Repository: &RepositoryProvider{
		Backend: "s3", Root: bucket + "/" + prefix,
		Options: map[string]string{
			"provider": "Other", "endpoint": scheme + endpoint,
			"access_key_id": accessKey, "secret_access_key": secretKey,
			"region": region, "force_path_style": "true", "no_check_bucket": "false",
		},
	}}
}
