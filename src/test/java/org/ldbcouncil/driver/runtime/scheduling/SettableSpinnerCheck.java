package org.ldbcouncil.driver.runtime.scheduling;

import org.ldbcouncil.driver.Operation;

public class SettableSpinnerCheck implements SpinnerCheck
{
    private SpinnerCheckResult result;

    public SettableSpinnerCheck( SpinnerCheckResult result )
    {
        this.result = result;
    }

    public void setResult( SpinnerCheckResult result )
    {
        this.result = result;
    }

    @Override
    public SpinnerCheckResult doCheck( Operation operation )
    {
        return result;
    }

    @Override
    public boolean handleFailedCheck( Operation operation )
    {
        return false;
    }
}
