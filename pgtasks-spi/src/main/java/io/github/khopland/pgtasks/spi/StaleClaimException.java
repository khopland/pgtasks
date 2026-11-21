package io.github.khopland.pgtasks.spi;

import java.sql.SQLException;
import java.util.Set;

/**
 * A completion matched fewer queue rows than it should: another node reclaimed some tasks after
 * this node was presumed dead, or they were changed by an operator. The completion transaction
 * must roll back, together with the child tasks and JDBC effects it carried.
 */
public class StaleClaimException extends SQLException {

    private final Set<Long> lost;

    public StaleClaimException(Set<Long> lost) {
        super("Claim lost for tasks " + lost + ": they were reclaimed or changed since this node claimed them");
        this.lost = Set.copyOf(lost);
    }

    public Set<Long> lost() {
        return lost;
    }
}
