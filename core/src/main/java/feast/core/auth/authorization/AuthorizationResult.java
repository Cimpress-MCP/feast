package feast.core.auth.authorization;

import java.util.Optional;
import javax.annotation.Nullable;
import com.google.auto.value.AutoValue;


@AutoValue
public abstract class AuthorizationResult {
  public static AuthorizationResult create(@Nullable boolean allowed, @Nullable String failureReason) {
    return new AutoValue_AuthorizationResult(allowed, Optional.of(failureReason));
  }
  
  public static AuthorizationResult failed(@Nullable String failureReason) {
    return new AutoValue_AuthorizationResult(false, Optional.of(failureReason));
  }
  
  public static AuthorizationResult success() {
    return new AutoValue_AuthorizationResult(true, Optional.empty());
  }
  
  abstract boolean allowed();
  
  abstract Optional<String> failureReason();
}
