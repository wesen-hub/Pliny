import com.dcp.outlet.rt.KafkaRecordSource;
import com.dcp.outlet.rt.ProductionMessageDecoder;
import com.dcp.outlet.rt.RawMessage;
import com.dcp.outlet.rt.DecodedMessage;
import com.dcp.outlet.rt.MessageDecoder;

import java.util.List;

/**
 * 集群真实数据验证: 用生产解码器解析集群 Kafka dcp.cdc.target_db 的真实 TiCDC 消息
 * (java -cp dcp-outlet-rt.jar DecoderVerify.java 单文件源模式运行)。
 */
public class DecoderVerify {
    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "dcp.cdc.target_db";
        int maxMsgs = args.length > 2 ? Integer.parseInt(args[2]) : 5;
        // 分区列表(逗号分隔, 缺省 0; 打标事件流多分区时传 0,1,2)
        java.util.List<Integer> parts = new java.util.ArrayList<>();
        String ps = args.length > 3 ? args[3] : "0";
        for (String s : ps.split(",")) parts.add(Integer.parseInt(s.trim()));

        KafkaRecordSource source = new KafkaRecordSource(bootstrap, "decoder-verify-" + System.currentTimeMillis(),
                topic, parts);
        for (int p : parts) source.seek(p, 0);   // 从头读(演示 topic 只有历史消息)
        MessageDecoder decoder = new ProductionMessageDecoder();

        int decoded = 0;
        long deadline = System.currentTimeMillis() + 15000;
        while (decoded < maxMsgs && System.currentTimeMillis() < deadline) {
            List<RawMessage> batch = source.poll(2000);
            for (RawMessage raw : batch) {
                DecodedMessage d = decoder.decode(raw);
                if (d.kind() != DecodedMessage.Kind.EVENT) {
                    System.out.println("SKIP kind=" + d.kind() + " p" + raw.partition() + "@" + raw.offset());
                    continue;
                }
                decoded++;
                System.out.println("EVENT p" + d.partition() + "@" + d.offset()
                        + " op=" + d.op() + " tso=" + d.tso() + " stream=" + d.stream()
                        + " key=" + d.key()
                        + " after=" + (d.after() == null ? null : d.after().entrySet().stream()
                                .limit(6).map(e -> e.getKey() + "=" + e.getValue()).toList())
                        + " before=" + (d.before() == null ? null : "{" + d.before().size() + " cols}"));
            }
        }
        source.close();
        System.out.println("RESULT decoded=" + decoded + " (topic=" + topic + ")");
        if (decoded == 0) {
            System.exit(2);   // 无真实消息可解 → 验证不通过
        }
    }
}
