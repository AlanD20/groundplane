import { ControllerRequestError } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import { useRef } from "react";

export function useGitDeliveryReplay() {
  const intents = useRef(
    new Map<string, { key: string; fingerprint: string }>(),
  );

  async function write<T>(
    action: string,
    fingerprint: string,
    send: (key: string) => Promise<T>,
  ) {
    let intent = intents.current.get(action);
    if (intent && intent.fingerprint !== fingerprint) {
      throw new Error(
        "The previous request is unconfirmed. Retry it with the same values before making another change.",
      );
    }
    if (!intent) {
      intent = { key: newULID(), fingerprint };
      intents.current.set(action, intent);
    }
    try {
      const accepted = await send(intent.key);
      intents.current.delete(action);
      return accepted;
    } catch (cause) {
      if (
        cause instanceof ControllerRequestError &&
        cause.code === "idempotency.in_progress"
      ) {
        throw new Error(
          "The Controller is still processing this request. Try again to check its result.",
          { cause },
        );
      }
      if (
        cause instanceof ControllerRequestError &&
        cause.status >= 400 &&
        cause.status < 500 &&
        cause.status !== 408 &&
        cause.status !== 429
      ) {
        intents.current.delete(action);
        throw cause;
      }
      throw new Error(
        "The Controller did not confirm the request. Retry to check whether it was accepted.",
        { cause },
      );
    }
  }

  return { write, pending: (action: string) => intents.current.has(action) };
}
