package net.openhft.chronicle.engine2.api;

/**
 * Created by daniel on 29/05/15.
 */
public class PermissionDeniedException extends RuntimeException {
    private Failure failure;

    public enum Failure {INCORRECT_TOKEN, INCORRECT_USER};

    public PermissionDeniedException() {
        super();
    }

    public PermissionDeniedException(Failure failure) {
        super();
        this.failure = failure;
    }

    public PermissionDeniedException(String message) {
        super(message);
    }

    public PermissionDeniedException(String message, Throwable cause) {
        super(message, cause);
    }

    public Failure getFailure() {
        return failure;
    }

    public void setFailure(Failure failure) {
        this.failure = failure;
    }
}
