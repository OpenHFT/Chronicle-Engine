package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.status.BadOwnerException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
class ChronicleNfsIdMapping implements NfsIdMapping {

    public static final NfsIdMapping EMPTY = new ChronicleNfsIdMapping();
    static final int USERID = Integer.getInteger("engine.nfs.userid", 1000);
    static final int GROUPID = Integer.getInteger("engine.nfs.groupid", 1000);

    private ChronicleNfsIdMapping() {
    }

    @Override
    public int principalToUid(String principal) throws BadOwnerException {
        return USERID;
    }

    @Override
    public int principalToGid(String principal) throws BadOwnerException {
        return GROUPID;
    }

    @NotNull
    @Override
    public String uidToPrincipal(int id) {
        return "nobody";
    }

    @NotNull
    @Override
    public String gidToPrincipal(int id) {
        return "nogroup";
    }

    @Override
    public String toString() {
        return "ChronicleNfsIdMapping{ userId = " + USERID + ", groupId= " + GROUPID + " }";
    }
}
