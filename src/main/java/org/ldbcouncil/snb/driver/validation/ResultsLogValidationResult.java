package org.ldbcouncil.snb.driver.validation;

import java.util.ArrayList;
import java.util.List;

public class ResultsLogValidationResult
{
    public enum ValidationErrorType
    {
        TOO_MANY_LATE_OPERATIONS,
        LATE_OPERATIONS_FOR_TYPE,
        UNEXPECTED
    }
    private boolean aboveThreshold = false;

    public static class ValidationError
    {
        private final ValidationErrorType errorType;
        private final String message;

        public ValidationError( ValidationErrorType errorType, String message )
        {
            this.errorType = errorType;
            this.message = message;
        }

        public ValidationErrorType errorType()
        {
            return errorType;
        }

        public String message()
        {
            return message;
        }
    }

    private final List<ValidationError> errors;

    public ResultsLogValidationResult()
    {
        this.errors = new ArrayList<>();
    }

    public void addError( ValidationErrorType validationErrorType, String errorMessage )
    {
        errors.add( new ValidationError( validationErrorType, errorMessage ) );
    }

    public void aboveThreshold()
    {
        this.aboveThreshold = true;
    }

    public boolean getAboveThreshold()
    {
        return this.aboveThreshold;
    }

    public List<ValidationError> errors()
    {
        return errors;
    }

    public boolean isSuccessful()
    {
        return !aboveThreshold;
    }

    public String getScheduleAuditResult(boolean recordDelayedOperations)
    {
        StringBuffer sb = new StringBuffer();
        if ( isSuccessful() )
        {
            sb.append( "PASSED SCHEDULE AUDIT -- workload operations executed to schedule" );
        }
        else
        {   recordDelayedOperations = true;
            sb.append( "FAILED SCHEDULE AUDIT -- errors:" );
        }
        if (recordDelayedOperations){
            for ( ValidationError error : errors )
            {
                sb.append( "\n\t" ).append( error.errorType().name() ).append( " : " ).append( error.message() );
            }
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        if ( isSuccessful() )
        {
            sb.append( "PASSED SCHEDULE AUDIT -- workload operations executed to schedule" );
        }
        else
        {
            sb.append( "FAILED SCHEDULE AUDIT -- errors:" );
            for ( ValidationError error : errors )
            {
                sb.append( "\n\t" ).append( error.errorType().name() ).append( " : " ).append( error.message() );
            }
        }
        return sb.toString();
    }
}
