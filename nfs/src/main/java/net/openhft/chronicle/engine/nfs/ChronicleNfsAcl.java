package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.v4.acl.Acls;
import org.dcache.nfs.v4.xdr.*;
import org.jetbrains.annotations.NotNull;

import static org.dcache.nfs.v4.xdr.nfs4_prot.*;

/**
 * @author Rob Austin.
 */
class ChronicleNfsAcl {

    @NotNull
    public static nfsace4[] getAcl() {
        return new nfsace4[]{
                toACE(Acls.OWNER, ACE4_ACCESS_ALLOWED_ACE_TYPE, ACE4_READ_DATA),
                toACE(Acls.GROUP, ACE4_ACCESS_ALLOWED_ACE_TYPE, ACE4_READ_DATA),
                toACE(Acls.OWNER, ACE4_ACCESS_ALLOWED_ACE_TYPE, ACE4_WRITE_ACL),
                toACE(Acls.OWNER, ACE4_ACCESS_ALLOWED_ACE_TYPE, ACE4_WRITE_RETENTION_HOLD),
                toACE(Acls.OWNER, ACE4_ACCESS_ALLOWED_ACE_TYPE, ACE4_DELETE)
        };
    }

    @NotNull
    private static nfsace4 toACE(utf8str_mixed principal, int type, int mask) {
        return toACE(principal, type, mask, 0);
    }

    @NotNull
    private static nfsace4 toACE(utf8str_mixed principal, int type, int mask, int flag) {
        nfsace4 ace = new nfsace4();
        ace.who = principal;
        ace.access_mask = new acemask4(new uint32_t(mask));
        ace.type = new acetype4(new uint32_t(type));
        int flags = flag | (principal == Acls.GROUP ? ACE4_IDENTIFIER_GROUP : 0);
        ace.flag = new aceflag4(new uint32_t(flags));
        return ace;
    }

}
