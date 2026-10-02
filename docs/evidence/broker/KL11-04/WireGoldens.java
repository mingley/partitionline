import java.nio.ByteBuffer;
import java.nio.file.*;
import java.net.InetAddress;
import java.util.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.network.*;
import org.apache.kafka.common.security.auth.*;
import org.apache.kafka.common.errors.*;

public final class WireGoldens {
    static final List<Map<String,Object>> entries = new ArrayList<>();
    static Path out;
    static byte[] data(Message m, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer b=ByteBuffer.allocate(m.size(cache,version));
        m.write(new ByteBufferAccessor(b),cache,version);
        if (b.hasRemaining()) throw new AssertionError("size/write mismatch");
        return b.array();
    }
    static byte[] concat(byte[]... chunks) {
        int n=0;for(byte[] b:chunks)n+=b.length;
        ByteBuffer result=ByteBuffer.allocate(n);for(byte[] b:chunks)result.put(b);
        return result.array();
    }
    static String hex(byte[] bytes) {
        StringBuilder s=new StringBuilder();for(byte b:bytes)s.append(String.format("%02x",b&255));return s.toString();
    }
    static byte[] frame(byte[] bytes) { return concat(ByteBuffer.allocate(4).putInt(bytes.length).array(),bytes); }
    static Map<String,Object> parse(byte[] request) {
        Map<String,Object> m=new LinkedHashMap<>();ByteBuffer b=ByteBuffer.wrap(request);
        try {
            RequestHeader h=RequestHeader.parse(b);
            m.put("header_accepted",true);m.put("header_version",h.headerVersion());
            m.put("api_key",h.apiKey().id);m.put("api_version",h.apiVersion());m.put("correlation_id",h.correlationId());m.put("client_id_normalized",h.clientId());m.put("header_consumed",b.position());
            if(h.apiKey()==ApiKeys.API_VERSIONS) {
                RequestContext context=new RequestContext(h,"golden",InetAddress.getLoopbackAddress(),KafkaPrincipal.ANONYMOUS,new ListenerName("PLAINTEXT"),SecurityProtocol.PLAINTEXT,new ClientInformation("fixture","1"),false);
                ApiVersionsRequest req=(ApiVersionsRequest)context.parseRequest(b).request;
                m.put("body_accepted",true);m.put("body_remaining",b.remaining());m.put("body_version",req.version());m.put("unsupported_request_version",req.hasUnsupportedRequestVersion());m.put("software_valid",req.isValid());
            }
        } catch(Throwable e) {
            if(!m.containsKey("header_accepted"))m.put("header_accepted",false);
            else m.put("body_accepted",false);
            m.put("exception",e.getClass().getName());m.put("message",String.valueOf(e.getMessage()));
        }
        return m;
    }
    static void save(String name,byte[] request,byte[] response,String expectation,String scope) throws Exception {
        Files.write(out.resolve(name+".request.bin"),request);
        if(response!=null)Files.write(out.resolve(name+".response.bin"),response);
        Map<String,Object> entry=new LinkedHashMap<>();entry.put("name",name);entry.put("request_hex",hex(request));entry.put("request_frame_hex",hex(frame(request)));entry.put("response_hex",response==null?null:hex(response));entry.put("response_frame_hex",response==null?null:hex(frame(response)));entry.put("broker_expectation",expectation);entry.put("scope",scope);entry.put("java_observed",parse(request));entries.add(entry);
    }
    static byte[] header(short version,int correlation,String client,boolean tags) {
        short hv=ApiKeys.API_VERSIONS.requestHeaderVersion(version);
        RequestHeaderData h=new RequestHeaderData().setRequestApiKey((short)18).setRequestApiVersion(version).setCorrelationId(correlation).setClientId(client);
        if(tags) { h.unknownTaggedFields().add(new RawTaggedField(2,new byte[]{1,2}));h.unknownTaggedFields().add(new RawTaggedField(7,new byte[]{})); }
        return data(h,hv);
    }
    static void valid(String name,short version,int correlation,String client,String software,String softwareVersion,boolean tags) throws Exception {
        ApiVersionsRequestData body=new ApiVersionsRequestData();if(version>=3)body.setClientSoftwareName(software).setClientSoftwareVersion(softwareVersion);
        if(tags)body.unknownTaggedFields().add(new RawTaggedField(8,new byte[]{3,4,5}));
        ApiVersionsRequest request=new ApiVersionsRequest(body,version);
        boolean acceptable=request.isValid();ApiVersionsResponse response;
        if(acceptable) {
            ApiVersionsResponseData.ApiVersionCollection keys=new ApiVersionsResponseData.ApiVersionCollection();keys.add(new ApiVersionsResponseData.ApiVersion().setApiKey((short)18).setMinVersion((short)0).setMaxVersion((short)4));
            response=new ApiVersionsResponse(new ApiVersionsResponseData().setErrorCode((short)0).setThrottleTimeMs(0).setApiKeys(keys));
        } else response=request.getErrorResponse(0,new InvalidRequestException("invalid software"));
        byte[] req=concat(header(version,correlation,client,tags),data(body,version));
        byte[] resp=concat(data(new ResponseHeaderData().setCorrelationId(correlation),(short)0),data(response.data(),version));
        save(name,req,resp,acceptable?"reply-success":"reply-invalid-request42","Independent Apache generated header/body serialization and request isValid semantics; only API18range0..4 selected in success fixture");
    }
    static void unsupported(String name,short version,int correlation) throws Exception {
        byte[] req=concat(header(version,correlation,"",false),new byte[]{(byte)255,(byte)128,42});
        ByteBuffer buffer=ByteBuffer.wrap(req);RequestHeader h=RequestHeader.parse(buffer);
        RequestContext context=new RequestContext(h,"golden",InetAddress.getLoopbackAddress(),KafkaPrincipal.ANONYMOUS,new ListenerName("PLAINTEXT"),SecurityProtocol.PLAINTEXT,new ClientInformation("fixture","1"),false);
        ApiVersionsRequest parsed=(ApiVersionsRequest)context.parseRequest(buffer).request;
        if(!parsed.hasUnsupportedRequestVersion()||parsed.version()!=0||context.apiVersion()!=0)throw new AssertionError("unsupported fallback");
        ApiVersionsResponse response=parsed.getErrorResponse(0,new UnsupportedVersionException("unsupported"));
        ByteBuffer serialized=context.buildResponseEnvelopePayload(response);byte[] bytes=new byte[serialized.remaining()];serialized.get(bytes);
        save(name,req,bytes,"reply-v0-unsupported-version35","Actual Apache RequestContext ignores unsupported ApiVersions body and serializes v0 fallback with supported API18 range");
    }
    static String json(Object v) {
        if(v==null)return "null";
        if(v instanceof Number||v instanceof Boolean)return v.toString();
        if(v instanceof Map) { StringJoiner s=new StringJoiner(",","{","}");for(Object x:((Map<?,?>)v).entrySet()){Map.Entry<?,?>e=(Map.Entry<?,?>)x;s.add(json(e.getKey().toString())+":"+json(e.getValue()));}return s.toString(); }
        if(v instanceof List){StringJoiner s=new StringJoiner(",","[","]");for(Object x:(List<?>)v)s.add(json(x));return s.toString();}
        StringBuilder s=new StringBuilder("\"");for(char c:v.toString().toCharArray()){switch(c){case '"':s.append("\\\"");break;case '\\':s.append("\\\\");break;case '\n':s.append("\\n");break;case '\r':s.append("\\r");break;case '\t':s.append("\\t");break;default:if(c<32)s.append(String.format("\\u%04x",(int)c));else s.append(c);}}return s.append('"').toString();
    }
    public static void main(String[] args) throws Exception {
        out=Path.of(args[0]);Files.createDirectories(out);
        valid("v0-named",(short)0,7,"partitionline","","",false);
        valid("v1-null-client",(short)1,-19,null,"","",false);
        valid("v2-empty-client",(short)2,Integer.MIN_VALUE,"","","",false);
        valid("v3-flexible",(short)3,Integer.MAX_VALUE,"π-client","partitionline","0.1.0",false);
        valid("v4-unknown-tags",(short)4,101,"partitionline","fixture","4.3.1",true);
        valid("v3-empty-software",(short)3,17,"client","","1",false);
        valid("v4-empty-software-version",(short)4,18,"client","fixture","",false);
        valid("v3-underscore-software",(short)3,19,"client","bad_name","1",false);
        valid("v4-trailing-dash-version",(short)4,20,"client","fixture","1-",false);
        unsupported("unsupported-v5",(short)5,102);
        unsupported("unsupported-negative-version",(short)-1,-102);
        byte[] body=data(new ApiVersionsRequestData().setClientSoftwareName("fixture").setClientSoftwareVersion("1"),(short)3);
        byte[] minimal=header((short)3,5,"",false);
        byte[] base=concat(minimal,body);
        for(int n=0;n<minimal.length;n++)save("header-truncated-"+n,Arrays.copyOf(base,n),null,"close","Independent Java truncation observation; no broker response body fabricated");
        byte[] classic=header((short)0,5,"",false);
        byte[] invalidUtf8=concat(Arrays.copyOf(classic,8),new byte[]{0,1,(byte)255});save("client-invalid-utf8",invalidUtf8,null,"close-strict-policy","Java replaces malformed UTF8; strict broker rejects invalid wire strings explicitly");
        byte[] negativeLength=classic.clone();negativeLength[8]=(byte)255;negativeLength[9]=(byte)254;save("client-length-minus2",negativeLength,null,"close-strict-policy","Java treats all negative lengths as nullable; strict broker accepts only defined null sentinel -1");
        byte[] prefix=Arrays.copyOf(minimal,minimal.length-1);
        save("header-duplicate-tags",concat(prefix,new byte[]{2,7,0,7,0},body),null,"close-strict-policy","Java generated unknown-tag reader accepts duplicates; writer requires strictly increasing tags");
        save("header-descending-tags",concat(prefix,new byte[]{2,9,0,7,0},body),null,"close-strict-policy","Java generated unknown-tag reader accepts descending tags; writer requires strictly increasing tags");
        save("header-tag-size-truncated",concat(prefix,new byte[]{1,7,100}),null,"close","Java reader rejects tag payload length beyond remaining bytes");
        save("header-tag-varint-overflow",concat(prefix,new byte[]{(byte)255,(byte)255,(byte)255,(byte)255,31},body),null,"close-strict-policy","Bounded checked unsigned varint/count; actual Java outcome retained");
        save("header-tag-varint-sixbytes",concat(prefix,new byte[]{(byte)128,(byte)128,(byte)128,(byte)128,(byte)128,0},body),null,"close","Overlong varint observation");
        byte[] unknown=classic.clone();unknown[0]=127;unknown[1]=(byte)255;save("unknown-api-key",unknown,null,"close","Unknown key has no universal Kafka error body; Java header parser rejects");
        byte[] known=classic.clone();known[0]=0;known[1]=3;save("unimplemented-metadata",known,null,"close-unimplemented-policy","Java recognizes Metadata header, but broker has no implemented Metadata handler and advertises no such API");
        save("supported-v0-trailing-body",concat(classic,new byte[]{1}),null,"close-strict-policy","Java request context may ignore trailing bytes for empty schema; strict frame-consumption policy recorded");
        save("v3-body-null-software",concat(minimal,new byte[]{0,2,'1',0}),null,"close","Nonnullable compact software name cannot be null");
        Map<String,Object> document=new LinkedHashMap<>();document.put("fixture_generator","WireGoldens.java using checksum-pinned Apache kafka-clients classes");document.put("api18_min",ApiKeys.API_VERSIONS.oldestVersion());document.put("api18_max",ApiKeys.API_VERSIONS.latestVersion());document.put("cases",entries);Files.writeString(out.resolve("goldens.json"),json(document)+"\n");
        System.out.println("generated "+entries.size()+" independent cases");
    }
}
