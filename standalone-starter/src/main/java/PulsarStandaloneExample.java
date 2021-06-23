import org.apache.pulsar.PulsarStandalone;
import org.apache.pulsar.PulsarStandaloneBuilder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;

import java.io.File;
import java.io.FileInputStream;

public class PulsarStandaloneExample {
    public static void main(String[] args) throws Exception {
        final File clusterConfigFile = new File(PulsarStandalone.class.getClassLoader().getResource("standalone.conf").toURI());
        ServiceConfiguration config =
            PulsarConfigurationLoader.create((new FileInputStream(clusterConfigFile)), ServiceConfiguration.class);
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        String zkServers = "127.0.0.1";
        config.setAdvertisedAddress("localhost");

        PulsarStandalone
            pulsarStandalone = PulsarStandaloneBuilder.instance().withConfig(config).withNoStreamStorage(true).build();

        if (config.getZookeeperServers() != null) {
            pulsarStandalone.setZkPort(Integer.parseInt(config.getZookeeperServers().split(":")[1]));
        }

        config.setZookeeperServers(zkServers + ":" + pulsarStandalone.getZkPort());
        config.setConfigurationStoreServers(zkServers + ":" + pulsarStandalone.getZkPort());

        config.setRunningStandalone(true);

        pulsarStandalone.setConfigFile(clusterConfigFile.getAbsolutePath());
        pulsarStandalone.setConfig(config);
        pulsarStandalone.start();

        PulsarAdmin admin =
            PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + config.getWebServicePort().get()).build();

        admin.topics().createPartitionedTopic("test-topic", 3);
    }
}
