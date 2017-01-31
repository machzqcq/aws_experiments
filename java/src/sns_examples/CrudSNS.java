package sns_examples;

import com.amazonaws.services.sns.AmazonSNSClient;

import java.util.List;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;

import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;

public class CrudSNS {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AmazonSNSClient snsClient = new AmazonSNSClient(new ClasspathPropertiesFileCredentialsProvider());
		snsClient.setRegion(Region.getRegion(Regions.US_EAST_1));

		// createTopic(snsClient,"MyNewTopic");
		// listTopics(snsClient);
//		subscribeToTopic(snsClient, "arn:aws:sns:us-east-1:170118904864:MyNewTopic");
//		publishToTopic(snsClient, "arn:aws:sns:us-east-1:170118904864:MyNewTopic");
		deleteTopic(snsClient, "arn:aws:sns:us-east-1:170118904864:MyNewTopic");

	}

	public static void createTopic(AmazonSNSClient snsClient, String topicName) {
		// create a new SNS client and set endpoint

		// create a new SNS topic
		CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
		CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);
		// print TopicArn
		System.out.println(createTopicResult);
		// get request id for CreateTopicRequest from SNS metadata
		System.out.println("CreateTopicRequest - " + snsClient.getCachedResponseMetadata(createTopicRequest));
	}

	public static List<String> listTopics(AmazonSNSClient snsClient) {
		ListTopicsResult listTopicsResult = snsClient.listTopics();
		String nextToken = listTopicsResult.getNextToken();
		List<Topic> topics = listTopicsResult.getTopics();
		List<String> topicArns = null;

		// ListTopicResult contains only 100 topics hence use next token to get
		// next 100 topics.
		while (nextToken != null) {
			listTopicsResult = snsClient.listTopics(nextToken);
			nextToken = listTopicsResult.getNextToken();
			topics.addAll(listTopicsResult.getTopics());
		}

		// Display all the Topic ARN's
		for (Topic topic : topics) {
			System.out.println("Arn:" + topic.getTopicArn());
//			topicArns.add(topic.getTopicArn());
			/*
			 * perform your actions here
			 */
		}
		return topicArns;
	}

	public static void subscribeToTopic(AmazonSNSClient snsClient, String topicArn) {
		// subscribe to an SNS topic
		SubscribeRequest subRequest = new SubscribeRequest(topicArn, "email", "seleniumfrmwrk@gmail.com");
		snsClient.subscribe(subRequest);
		// get request id for SubscribeRequest from SNS metadata
		System.out.println("SubscribeRequest - " + snsClient.getCachedResponseMetadata(subRequest));
		System.out.println("Check your email and confirm subscription.");
	}

	public static void publishToTopic(AmazonSNSClient snsClient, String topicArn) {
		// publish to an SNS topic
		String msg = "My text published to SNS topic with email endpoint";
		PublishRequest publishRequest = new PublishRequest(topicArn, msg);
		PublishResult publishResult = snsClient.publish(publishRequest);
		// print MessageId of message published to SNS topic
		System.out.println("MessageId - " + publishResult.getMessageId());
	}

	public static void deleteTopic(AmazonSNSClient snsClient, String topicArn) {
		// delete an SNS topic
		DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(topicArn);
		snsClient.deleteTopic(deleteTopicRequest);
		// get request id for DeleteTopicRequest from SNS metadata
		System.out.println("DeleteTopicRequest - " + snsClient.getCachedResponseMetadata(deleteTopicRequest));
	}

}
