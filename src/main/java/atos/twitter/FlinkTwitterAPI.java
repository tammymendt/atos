package atos.twitter;

import org.apache.flink.contrib.tweetinputformat.io.SimpleTweetInputFormat;
import org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet;
import org.apache.flink.core.fs.FileInputSplit;

import java.io.File;
import java.io.IOException;

import org.apache.flink.core.fs.Path;

public class FlinkTwitterAPI {

	private static SimpleTweetInputFormat tweetInput = new SimpleTweetInputFormat();

	private static File jsonFile = new File("/home/tamara/Desktop/HashTagTweetSample.json");

	public static void main(String[] args){

		FileInputSplit fileInputSplit = new FileInputSplit(0, new Path(jsonFile.getPath()), 0, jsonFile.length(), new String[]{"localhost"});

		try {

			tweetInput.open(fileInputSplit);

			Tweet tweet = new Tweet();
			while (true){
				tweet = tweetInput.nextRecord(tweet);
				if (tweet == null) {
					break;
				}
				System.out.println(tweet.getText());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
