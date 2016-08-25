package storm.spout;
import java.util.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestWordSpout extends BaseRichSpout{
	SpoutOutputCollector _collector;
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

 _collector = collector;
		
	}

	public void nextTuple() {
		Utils.sleep(100);
		final String[]words=new String[]{"nithan","mike","jackson","golda","bertels"};
		final Random rand=new Random();
		final String word=words[rand.nextInt(words.length)];
		_collector.emit(new Values(word));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
