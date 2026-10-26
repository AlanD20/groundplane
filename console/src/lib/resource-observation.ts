const listeners = new Set<() => void>();
let mutationVersion = 0;

export const controllerMutationVersion = () => mutationVersion;

export function reportResourceChange(mutation = false) {
  if (mutation) mutationVersion++;
  listeners.forEach((listener) => listener());
}

export function subscribeResourceChanges(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
