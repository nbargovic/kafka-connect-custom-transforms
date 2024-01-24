package io.confluent.kafka.connect.predicates;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class FieldIsIP<R extends ConnectRecord<R>> implements Predicate<R> {
    private final Logger log = LoggerFactory.getLogger(FieldIsIP.class);

    private static final String FIELD_CONFIG = "field";
    private static final String USE_VALUE_CONFIG = "useValue";
    public static final String OVERVIEW_DOC = "A predicate which is true for records with an IP address as the value of the configured field name.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "The field name to look for an IP address.")
            .define(USE_VALUE_CONFIG, ConfigDef.Type.BOOLEAN, false,
                    null, ConfigDef.Importance.LOW,
                    "Use the message value instead of the key to look for an IP address.");
    private String fieldName;
    private boolean useValue = false;

    private Pattern pattern;
    private Matcher matcher;

    private static final String IPADDRESS_PATTERN =
            "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        log.debug("Running FieldIsIP predicate using message " + (useValue ? "value" : "key") + " and field name of  "+ fieldName +"'");
        try {
            final Map<String, Object> value = useValue ? requireMap(record.value(), "") : requireMap(record.key(), "");
            String dataValue = String.valueOf(value.get(fieldName));
            dataValue = dataValue.replaceAll("\"","");
            boolean isIP = isIPAddress(dataValue);
            log.debug("isIPAddress() returned "+ isIP +" for value: "+ dataValue);
            return isIP;
        } catch (DataException ex) {
            log.warn("Unable to get a field named '"+ fieldName +"' from the kafka message.", ex);
            return false;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        fieldName = new SimpleConfig(config(), configs).getString(FIELD_CONFIG);
        useValue = new SimpleConfig(config(), configs).getBoolean(USE_VALUE_CONFIG);
        pattern = Pattern.compile(IPADDRESS_PATTERN);
    }

    private boolean isIPAddress(String checkMe) {
        matcher = pattern.matcher(checkMe);
        return matcher.matches();
    }
}
