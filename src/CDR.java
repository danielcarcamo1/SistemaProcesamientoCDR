public class CDR {

    //datos de las llamadas
    private final String accountNumber;
    private final String callingNumber;
    private final String calledNumber;
    private final String timestamp;
    private final int durationMinutes;
    private final double cost;
    private final String callType;

    //cosntructor
    public CDR(String accountNumber, String callingNumber, String calledNumber,
               String timestamp, int durationMinutes, double cost, String callType) {
        this.accountNumber = accountNumber;
        this.callingNumber = callingNumber;
        this.calledNumber = calledNumber;
        this.timestamp = timestamp;
        this.durationMinutes = durationMinutes;
        this.cost = cost;
        this.callType = callType;
    }

    //metodos getters
    public String getAccountNumber() { return accountNumber; }
    public String getCallingNumber() { return callingNumber; }
    public String getCalledNumber() { return calledNumber; }
    public String getTimestamp() { return timestamp; }
    public int getDurationMinutes() { return durationMinutes; }
    public double getCost() { return cost; }
    public String getCallType() { return callType; }

    @Override
    public String toString() {
        return accountNumber + "," + callingNumber + "," + calledNumber + "," +
                timestamp + "," + durationMinutes + "," + cost + "," + callType;
    }
}