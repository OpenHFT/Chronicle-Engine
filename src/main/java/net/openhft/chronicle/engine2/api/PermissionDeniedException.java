package net.openhft.chronicle.engine2.api;

/**
 * Created by daniel on 29/05/15.
 */
public class PermissionDeniedException extends RuntimeException {
    public PermissionDeniedException() {
        super();
    }

    public PermissionDeniedException(String message) {
        super(message);
    }

    public PermissionDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}
