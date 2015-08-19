package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.status.BadOwnerException;
import org.dcache.nfs.v4.NfsIdMapping;

/**
 * @author Rob Austin.
 */
public class ChronicleNfsIdMapping implements NfsIdMapping {

    public static final NfsIdMapping EMPTY = new ChronicleNfsIdMapping();

    private ChronicleNfsIdMapping() {
    }

    @Override
    public int principalToUid(String principal) throws BadOwnerException {
        return 65534;  // this is the id of my current unix user via "$id -u <username>"
    }

    @Override
    public int principalToGid(String principal) throws BadOwnerException {
        return 65534;   // this is the id of my current unix user via "$id -u <username>"
    }

    @Override
    public String uidToPrincipal(int id) {
        return "nobody";
    }

    @Override
    public String gidToPrincipal(int id) {
        return "nogroup";
    }
}
