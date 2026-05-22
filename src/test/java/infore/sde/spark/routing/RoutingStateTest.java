package infore.sde.spark.routing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RoutingStateTest {

    @Test
    void initialStateIsEmpty() {
        RoutingState state = new RoutingState();
        assertThat(state.getRegistrations()).isEmpty();
    }

    @Test
    void registerAndUnregister() {
        RoutingState state = new RoutingState();
        state.getRegistrations().put(42,
                new RoutingState.RoutingRegistration(1, 2, "Forex"));
        assertThat(state.getRegistrations()).containsKey(42);

        RoutingState.RoutingRegistration removed = state.getRegistrations().remove(42);
        assertThat(removed.getNoOfP()).isEqualTo(2);
        assertThat(removed.getDataSetKey()).isEqualTo("Forex");
        assertThat(state.getRegistrations()).isEmpty();
    }
}
