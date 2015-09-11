/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.cfg.Installable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.dcache.xdr.OncRpcSvc;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by peter on 26/08/15.
 */
public class NfsCfg implements Installable, Marshallable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NfsCfg.class);
    private boolean enabled;
    private boolean debug;
    private Map<String, String> exports = new LinkedHashMap<>();
    private OncRpcSvc oncRpcSvc;

    @Override
    public NfsCfg install(String path, AssetTree assetTree) throws IOException, URISyntaxException {
        if (enabled) {
            LOGGER.info("Enabling NFS for " + assetTree);
            File exports = File.createTempFile("exports", "");
            exports.deleteOnExit();
            try (PrintWriter pw = new PrintWriter(exports)) {
                for (Map.Entry<String, String> entry : this.exports.entrySet()) {
                    pw.append(entry.getKey()).append("    ").append(entry.getValue()).append("\n");
                }
            }
            oncRpcSvc = ChronicleNfsServer.start(assetTree, exports.toString(), debug);
        }
        return this;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "enabled").bool(this, (o, b) -> o.enabled = b)
                .read(() -> "debug").bool(this, (o, b) -> o.debug = b)
                .read(() -> "exports").marshallable(w -> {
            StringBuilder name = new StringBuilder();
            while (w.hasMore()) {
                w.read(name).text(exports, (e, s) -> e.put(name.toString(), s));
            }
        });
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "enabled").bool(enabled)
                .write(() -> "debug").bool(debug)
                .write(() -> "exports").marshallable(w -> {
            for (Map.Entry<String, String> entry : exports.entrySet()) {
                w.write(entry::getKey).text(entry.getValue());
            }
        });
    }

    @Override
    public String toString() {
        return "NfsCfg{" +
                "enabled=" + enabled +
                ", debug=" + debug +
                ", exports=" + exports +
                '}';
    }
}
