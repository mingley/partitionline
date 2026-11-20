/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;

/** Read the generated Metadata body ranges from a pinned SDK. */
public final class ConformanceMetadataVersions {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("<jar>");
        Path jar = Path.of(args[0]);
        for (var type : new Class<?>[] {MetadataRequestData.class, MetadataResponseData.class}) {
            Path loaded = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isSameFile(jar, loaded)) throw new AssertionError("wrong Kafka SDK loaded");
        }
        var request = new MetadataRequestData();
        var response = new MetadataResponseData();
        System.out.println("metadata_declared=request:" + request.lowestSupportedVersion()
            + "-" + request.highestSupportedVersion() + ",response:"
            + response.lowestSupportedVersion() + "-" + response.highestSupportedVersion());
    }
}
