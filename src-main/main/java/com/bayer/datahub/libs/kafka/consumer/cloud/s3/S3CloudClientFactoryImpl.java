package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

class S3CloudClientFactoryImpl implements CloudClientFactory {
    private static final Logger log = LoggerFactory.getLogger(S3CloudClientFactoryImpl.class);
    private final String serviceEndpoint;
    private final S3AuthType authType;
    private final String roleArn;
    private final String roleSession;
    private final String accessKey;
    private final String secretKey;
    private final String region;

    @Inject
    public S3CloudClientFactoryImpl(Configs configs) {
        serviceEndpoint = configs.consumerS3AwsServiceEndpoint;
        authType = S3AuthType.parse(configs.consumerS3AwsAuthType);
        roleArn = configs.consumerS3AwsAuthRoleArn;
        accessKey = configs.consumerS3AwsAuthBasicAccessKey;
        roleSession = configs.consumerS3AwsAuthRoleSession;
        secretKey = configs.consumerS3AwsAuthBasicSecretKey;
        region = configs.consumerS3AwsRegion;
    }

    @Override
    public AmazonS3 createClient() {
        log.debug("Creating AmazonS3 client with {} auth type...", authType);
        switch (authType) {
            case AUTO:
                return autoAuth();
            case BASIC:
                return basicAuth();
            case ROLE:
                return roleAuth();
            default:
                throw new IllegalArgumentException("Unsupported auth type: " + authType);
        }
    }

    private AmazonS3 autoAuth() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(region))
                .build();
    }

    private AmazonS3 basicAuth() {
        var credentials = new BasicAWSCredentials(accessKey, secretKey);
        var endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, Regions.fromName(region).getName());
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(endpointConfiguration)
                .enablePathStyleAccess()
                .build();
    }

    private AmazonS3 roleAuth() {
        var roleRequest = new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withRoleSessionName(roleSession);

        var stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withRegion(Regions.fromName(region))
                .build();
        var assumeResult = stsClient.assumeRole(roleRequest);
        stsClient.shutdown();

        var temporaryCredentials = new BasicSessionCredentials(
                assumeResult.getCredentials().getAccessKeyId(),
                assumeResult.getCredentials().getSecretAccessKey(),
                assumeResult.getCredentials().getSessionToken());

        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(temporaryCredentials))
                .withRegion(Regions.fromName(region))
                .build();
    }
}
