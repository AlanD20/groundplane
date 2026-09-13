import type { components } from './api.generated'

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends
  (<Value>() => Value extends Right ? 1 : 2)
    ? true
    : false
type Expect<Value extends true> = Value

// Delivery: compile-time generated error keys, not runtime error handling.
// Rationale: Console code must receive the generated closed RFC 7807 tuple;
// an open or extra key would hide drift in the code-first Controller contract.
type ProblemKeysAreExact = Expect<
  Equal<keyof components['schemas']['Error'], 'type' | 'title' | 'status' | 'detail' | 'code'>
>

export type { ProblemKeysAreExact }
