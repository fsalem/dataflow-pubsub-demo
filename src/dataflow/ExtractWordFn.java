package dataflow;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class ExtractWordFn extends DoFn<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 311222424158594211L;

	@Override
	public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
		System.out.println("Message: "+c.element());
		for (String word : c.element().toLowerCase().split("\\W+")) {
			if (!word.isEmpty()) {
				c.output(word);
			}
		}

	}

}
