package com.test.Lambdatest.eventHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;

/*
 * This class will record S3 event and automate the process 
 */
public class Handler implements RequestHandler<S3Event, String> {

	@Override
	public String handleRequest(S3Event event, Context context) {
		try {
			StringBuffer sbf = new StringBuffer();
			for (S3EventNotificationRecord record : event.getRecords()) {
				String srcBucket = record.getS3().getBucket().getName();
				// Object key may have spaces or unicode non-ASCII characters.
				String srcKey = record.getS3().getObject().getKey()
						.replace('+', ' ');
				srcKey = URLDecoder.decode(srcKey, "UTF-8");
				// Detect file type
				Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(
						srcKey);
				if (!matcher.matches()) {

					System.out.println("Unable to detect file type for key "
							+ srcKey);
					return "";
				}
				AmazonS3 s3Client = new AmazonS3Client();
				S3Object body = s3Client.getObject(srcBucket, srcKey);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(body.getObjectContent()));
				String line;
				while ((line = reader.readLine()) != null) {
					sbf.append(line);
				}
				System.out.println("File Content: " + sbf.toString());
				reader.close();
				return sbf.toString();

			}

		} catch (Exception exp) {
			System.out.println("Error Message : " + exp.getMessage());
		}
		return "failed";
	}
}
