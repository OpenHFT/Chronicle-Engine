package net.openhft.chronicle.engine2.api;

/**
 * Created by daniel on 29/05/15.
 */
public class VanillaPermissions implements Permissions {
    private boolean readOnly, authenticated;
    @Override
    public boolean isReadOnly() {
        return false;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }
}
