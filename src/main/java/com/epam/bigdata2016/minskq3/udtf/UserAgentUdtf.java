package com.epam.bigdata2016.minskq3.udtf;


import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


import java.util.ArrayList;
import java.util.Arrays;

@UDFType(deterministic = false)
@Description(
        name="agent_pars_udf",
        value="Parse user-agent string into separate fields. v.1.4.1",
        extended="Adds UA_TYPE, UA_FAMILY, OS_NAME, DEVICE based on user-agent."
)
public class UserAgentUdtf extends GenericUDTF {

    private static final String[] fields = new String[]{
            "UA_TYPE","UA_FAMILY","OS_NAME","DEVICE"
    };
    private static final StructObjectInspector inspector;
    private PrimitiveObjectInspector agentDtlOI = null;
    private Object[] fwdObj = null;

    //Inspector initializing
    static {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        Arrays.stream(fields).forEach(field -> {
            fieldNames.add(field);
            fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                    PrimitiveObjectInspector.PrimitiveCategory.STRING));
        });
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldOIs);
    }


    public StructObjectInspector initialize(ObjectInspector[] arg) {
        agentDtlOI = (PrimitiveObjectInspector) arg[0];
        fwdObj = new Object[4];
        return inspector;
    }

    @Override
    public void process(Object[] arg) throws HiveException {
        String agentDtl = agentDtlOI.getPrimitiveJavaObject(arg[0]).toString();
        UserAgent ua = UserAgent.parseUserAgentString(agentDtl);

        // UA type
        fwdObj[0] = ua.getBrowser().getBrowserType().getName();
        // UA family
        fwdObj[1] = ua.getBrowser().getGroup().getName();
        // OS name
        fwdObj[2] = ua.getOperatingSystem().getName();
        // Device
        fwdObj[3] = ua.getOperatingSystem().getDeviceType().getName();

        this.forward(fwdObj);

    }


    @Override
    public void close() throws HiveException {
    }
}
