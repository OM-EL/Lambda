package handler;


import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.S3Client;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Handler implements RequestHandler<S3Event, String> {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final float MAX_DIMENSION_THUMB = 100;
    private static final float MAX_DIMENSION_LARGE = 2000;
    private final String REGEX = ".*\\.([^\\.]*)";
    private final String JPG_TYPE = "jpg";
    private final String JPG_MIME = "image/jpeg";
    private final String PNG_TYPE = "png";
    private final String PNG_MIME = "image/png";

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        final LambdaLogger logger = context.getLogger();
        try {
            logger.log("EVENT: " + gson.toJson(s3event));
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getUrlDecodedKey();

            String dstBucketThumb = srcBucket + "-thumb";
            String dstBucketLarge = srcBucket + "-large";
            String dstKey = srcKey;

            Matcher matcher = Pattern.compile(REGEX).matcher(srcKey);
            if (!matcher.matches()) {
                logger.log("Unable to infer image type for key " + srcKey);
                return "";
            }
            String imageType = matcher.group(1).toLowerCase();;
            if (!(JPG_TYPE.equals(imageType)) && !(PNG_TYPE.equals(imageType))) {
                logger.log("Skipping non-image " + srcKey);
                return "";
            }

            S3Client s3Client = S3Client.builder().build();
            InputStream s3Object = getObject(s3Client, srcBucket, srcKey);

            BufferedImage srcImage = ImageIO.read(s3Object);
            BufferedImage thumbImage = resizeImage(srcImage, MAX_DIMENSION_THUMB);
            ByteArrayOutputStream outputStreamThumb = new ByteArrayOutputStream();
            ImageIO.write(thumbImage, imageType, outputStreamThumb);
            putObject(s3Client, outputStreamThumb, dstBucketThumb, dstKey, imageType, logger);
            logger.log("Successfully created thumbnail at " + dstBucketThumb + "/" + dstKey);

            BufferedImage largeImage = resizeImage(srcImage, MAX_DIMENSION_LARGE);
            ByteArrayOutputStream outputStreamLarge = new ByteArrayOutputStream();
            ImageIO.write(largeImage, imageType, outputStreamLarge);
            putObject(s3Client, outputStreamLarge, dstBucketLarge, dstKey, imageType, logger);
            logger.log("Successfully created large version at " + dstBucketLarge + "/" + dstKey);

            return "Ok";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getObject(S3Client s3Client, String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return s3Client.getObject(getObjectRequest);
    }

    private void putObject(S3Client s3Client, ByteArrayOutputStream outputStream,
                           String bucket, String key, String imageType, LambdaLogger logger) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("Content-Length", Integer.toString(outputStream.size()));
        if (JPG_TYPE.equals(imageType)) {
            metadata.put("Content-Type", JPG_MIME);
        } else if (PNG_TYPE.equals(imageType)) {
            metadata.put("Content-Type", PNG_MIME);
        }

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .acl(ObjectCannedACL.PUBLIC_READ)
                .metadata(metadata)
                .build();

        logger.log("Writing to: " + bucket + "/" + key);
        try {
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(outputStream.toByteArray()));
        } catch (AwsServiceException e) {
            logger.log(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private BufferedImage resizeImage(BufferedImage srcImage, float maxDimension) {
        int srcHeight = srcImage.getHeight();
        int srcWidth = srcImage.getWidth();
        float scalingFactor = Math.min(maxDimension / srcWidth, maxDimension / srcHeight);
        int width = (int) (scalingFactor * srcWidth);
        int height = (int) (scalingFactor * srcHeight);

        BufferedImage resizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2d = resizedImage.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g2d.drawImage(srcImage, 0, 0, width, height, null);
        g2d.dispose();

        return resizedImage;
    }
}
