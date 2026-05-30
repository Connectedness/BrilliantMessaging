namespace Usf.Core.Messaging.Errors;

public enum MessageDeliveryFailureReason
{
    Nacked = 0,
    Returned = 1,
    Timeout = 2
}
