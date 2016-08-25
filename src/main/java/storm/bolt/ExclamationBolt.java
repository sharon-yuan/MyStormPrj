package storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBolt extends BaseRichBolt{
OutputCollector _collector;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector=collector;
	}

	public void execute(Tuple tuple) {
		this._collector.emit(tuple,new Values(tuple.getString(0)+"!!!"));
		this._collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
