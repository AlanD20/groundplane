import { createRoot } from "react-dom/client";
import { useEffect, useRef } from "react";
import {
  type EditorView,
  type Panel,
  runScopeHandlers,
} from "@codemirror/view";
import {
  SearchQuery,
  getSearchQuery,
  setSearchQuery,
  findNext,
  findPrevious,
  replaceNext,
  replaceAll,
  selectMatches,
  closeSearchPanel,
} from "@codemirror/search";
import { ArrowDown, ArrowUp, X } from "lucide-react";
import { Button } from "./button";
import { Checkbox } from "./checkbox";
import { Input } from "./input";

function SearchControls({ view }: { view: EditorView }) {
  const query = getSearchQuery(view.state);
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => {
    input.current?.focus();
    input.current?.select();
  }, []);
  const commit = (patch: Partial<SearchQuery>) => {
    view.dispatch({
      effects: setSearchQuery.of(
        new SearchQuery({
          search: query.search,
          replace: query.replace,
          caseSensitive: query.caseSensitive,
          regexp: query.regexp,
          wholeWord: query.wholeWord,
          literal: query.literal,
          ...patch,
        }),
      ),
    });
  };
  return (
    <div
      className="space-y-2 p-3 font-sans text-sm"
      onKeyDown={(event) => {
        if (runScopeHandlers(view, event.nativeEvent, "search-panel"))
          event.preventDefault();
        else if (
          event.key === "Enter" &&
          event.target instanceof HTMLInputElement
        ) {
          event.preventDefault();
          if (event.target.name === "replace") replaceNext(view);
          else (event.shiftKey ? findPrevious : findNext)(view);
        }
        // Editor shortcuts must never submit the surrounding resource form.
        if (event.key === "Enter") event.stopPropagation();
      }}
    >
      <div className="flex flex-wrap items-center gap-2">
        <Input
          ref={input}
          {...{ "main-field": "true" }}
          form=""
          name="search"
          aria-label="Find"
          placeholder="Find"
          className="min-w-0 flex-1 basis-40"
          value={query.search}
          aria-invalid={!!query.search && !query.valid}
          onChange={(event) => commit({ search: event.target.value })}
        />
        <Button
          type="button"
          variant="outline"
          size="icon-sm"
          aria-label="Previous match"
          disabled={!query.valid}
          onClick={() => findPrevious(view)}
        >
          <ArrowUp />
        </Button>
        <Button
          type="button"
          variant="outline"
          size="icon-sm"
          aria-label="Next match"
          disabled={!query.valid}
          onClick={() => findNext(view)}
        >
          <ArrowDown />
        </Button>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={!query.valid}
          onClick={() => selectMatches(view)}
        >
          Select all
        </Button>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          aria-label="Close search"
          onClick={() => {
            closeSearchPanel(view);
            view.focus();
          }}
        >
          <X />
        </Button>
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-2">
        {(
          [
            ["caseSensitive", "Match case"],
            ["wholeWord", "Whole word"],
            ["regexp", "Regular expression"],
          ] as const
        ).map(([key, label]) => (
          <label key={key} className="inline-flex items-center gap-2">
            <Checkbox
              form=""
              checked={query[key]}
              onChange={(event) => commit({ [key]: event.target.checked })}
            />
            {label}
          </label>
        ))}
      </div>
      {query.search && !query.valid && (
        <p role="alert" className="text-xs text-destructive">
          Invalid regular expression.
        </p>
      )}
      {!view.state.readOnly && (
        <div className="flex flex-wrap items-center gap-2">
          <Input
            form=""
            name="replace"
            aria-label="Replace with"
            placeholder="Replace with"
            className="min-w-0 flex-1 basis-40"
            value={query.replace}
            onChange={(event) => commit({ replace: event.target.value })}
          />
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={!query.valid}
            onClick={() => replaceNext(view)}
          >
            Replace
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={!query.valid}
            onClick={() => replaceAll(view)}
          >
            Replace all
          </Button>
        </div>
      )}
    </div>
  );
}

/** CodeMirror owns search state and commands; Console owns the panel controls. */
export function createEditorSearchPanel(view: EditorView): Panel {
  const dom = document.createElement("div");
  const root = createRoot(dom);
  const render = () => root.render(<SearchControls view={view} />);
  return {
    dom,
    top: true,
    mount: render,
    update: render,
    destroy() {
      queueMicrotask(() => root.unmount());
    },
  };
}
