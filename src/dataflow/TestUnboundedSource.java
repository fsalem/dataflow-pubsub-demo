package dataflow;

import org.joda.time.Duration;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class TestUnboundedSource {

	public static void main(String[] args) {
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(
				DataflowPipelineOptions.class);
		options.setRunner(DataflowPipelineRunner.class);
		options.setProject("rapid-stream-118713");
		options.setStagingLocation("gs://streaming-test");
		options.setStreaming(Boolean.TRUE);
		options.setMaxNumWorkers(1);
		
		Pipeline p = Pipeline.create(options);
		p.begin();

		PCollection<String> input = p.apply(PubsubIO.Read
		// .topic("projects/rapid-stream-118713/topics/tweets")
				.subscription(
						"projects/rapid-stream-118713/subscriptions/getTweets")
				// .timestampLabel(Long.toString(System.currentTimeMillis()))
				.named("Get Tweets"));
		PCollection<String> windowInput = input
		// .apply(Window.<String>
		// into(FixedWindows.of(Duration.standardMinutes(1))));
				.apply(Window
						.<String> into(
								FixedWindows.of(Duration.standardMinutes(1)))
						.triggering(
								AfterProcessingTime.pastFirstElementInPane()
						// .plusDelayOf(
						// Duration.standardMinutes(1))
						).discardingFiredPanes()
						.withAllowedLateness(Duration.standardMinutes(1)));

		windowInput.apply(new WordCountTransform()).apply(
				PubsubIO.Write.topic(
						"projects/rapid-stream-118713/topics/wordCount").named(
						"Write counts"));
		p.run();

	}

}
