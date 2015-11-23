import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class s3du {

    static AmazonS3 s3;

    private static void init() throws Exception {
        File configFile = new File(System.getProperty("user.home"), ".aws/credentials");
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(
                new ProfilesConfigFile(configFile), "default");

        if (credentialsProvider.getCredentials() == null) {
            throw new RuntimeException("No AWS security credentials found:\n"
                    + "Make sure you've configured your credentials in: " + configFile.getAbsolutePath() + "\n"
                    + "For more information on configuring your credentials, see "
                    + "http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html");
        }

        s3 = new AmazonS3Client(credentialsProvider);
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        Map<K, V> result = new LinkedHashMap<>();
        map.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getValue()))
                .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

        return result;
    }

    public static void main(String[] args) throws Exception {
        init();

        final int s3ObjectLimit = 0; // max objects per bucket limit (for testing)
        final int maxDepth = 3; // max "directory" depth to report

        try {
            List<Bucket> buckets = s3.listBuckets();
            System.out.println(buckets.size() + " Amazon S3 bucket(s) detected.");
            s3.listBuckets().stream()
                    .forEach(bucket -> {
                                ConcurrentHashMap<String, Long> keyMap = new ConcurrentHashMap<>();
                                int objectCount = 0;
                                for (S3ObjectSummary s3ObjectSummary : S3Objects.inBucket(s3, bucket.getName())) {
                                    objectCount++;
                                    final StringBuilder s3Path = new StringBuilder();
                                    final String[] splitKey = (s3ObjectSummary.getKey().split("/"));
                                    String separator = "/";
                                    int splitKeyLength = splitKey.length - 1;
                                    if (splitKeyLength == 0) {
                                        splitKeyLength = 1;
                                        separator = "";
                                    }
                                    ;
                                    for (int i = 0; i < splitKeyLength; i++) {
                                        s3Path.append(splitKey[i] + separator);
                                        final String keyPath = s3Path.toString();
                                        keyMap.putIfAbsent(keyPath, 0L);
                                        keyMap.put(keyPath, keyMap.get(keyPath) + s3ObjectSummary.getSize());
                                    }

                                    if (s3ObjectLimit > 0 && objectCount > s3ObjectLimit) {
                                        break;
                                    }
                                }
                                ;
                                // Display output
                                System.out.println("bucket: " + bucket.getName());
                                System.out.println("objects: " + objectCount);
                                Map<String, Long> sortedKeyMap = sortByValue(keyMap);
                                List<String> sortedKeys = new ArrayList(sortedKeyMap.keySet());
                                Collections.reverse(sortedKeys);
                                sortedKeys.stream()
                                        .filter(s -> {
                                            return (s.split("/").length <= maxDepth);
                                        })
                                        .forEachOrdered(key -> {
                                            System.out.println(sortedKeyMap.get(key) + " : " + key);
                                        });
                            }
                    );
        } catch (AmazonServiceException ase) {
            /*
             * AmazonServiceExceptions represent an error response from an AWS
             * services, i.e. your request made it to AWS, but the AWS service
             * either found it invalid or encountered an error trying to execute
             * it.
             */
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            /*
             * AmazonClientExceptions represent an error that occurred inside
             * the client on the local host, either while trying to send the
             * request to AWS or interpret the response. For example, if no
             * network connection is available, the client won't be able to
             * connect to AWS to execute a request and will throw an
             * AmazonClientException.
             */
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
