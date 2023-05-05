-define(IS_TIMEOUT(T),
    (T == infinity orelse is_integer(T) andalso T > 0)
).
-define(DEFAULT_TIMEOUT, 15000).


-type optional(T) :: T | undefined.