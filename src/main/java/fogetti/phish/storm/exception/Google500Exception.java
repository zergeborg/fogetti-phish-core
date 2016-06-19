package fogetti.phish.storm.exception;

public class Google500Exception extends RuntimeException {

    private static final long serialVersionUID = -4316336579529510627L;

    public Google500Exception() {
        super("Google responded with HTTP 500");
    }
}