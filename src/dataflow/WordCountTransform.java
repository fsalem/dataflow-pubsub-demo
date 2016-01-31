package dataflow;

import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WordCountTransform extends
		PTransform<PCollection<String>, PCollection<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1874122535644822187L;

	@Override
	public PCollection<String> apply(PCollection<String> lines) {
		return lines.apply(ParDo.of(new ExtractWordFn()))
				.apply(Count.<String> perElement())
				.apply(ParDo.of(new CombineWordCountFn()));
	}
}
