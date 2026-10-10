import {
  ArrowUpRight,
  Boxes,
  Layers,
  Terminal,
  Server,
  Code2,
  Globe,
  Workflow,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { CopyButton } from "@/components/common/copy-button";
import { ThemeToggle } from "@/components/common/theme-toggle";

const repository = "https://github.com/AlanD20/groundplane";
const command = "curl -fsSL https://gp.aland20.com/install.sh | sudo bash";

export function LandingPage() {
  return (
    <div className="public-site mx-auto max-w-7xl px-5 sm:px-8 lg:px-12">
      <a href="#main" className="site-skip">
        Skip to content
      </a>
      <header className="flex flex-wrap items-center justify-between gap-4 border-b border-border py-5">
        <a
          href="/"
          className="flex items-center gap-2.5 text-lg font-semibold tracking-tight"
        >
          <Layers className="size-6 text-primary" strokeWidth={1.6} />
          Groundplane
        </a>
        <nav
          aria-label="Main navigation"
          className="flex flex-wrap items-center gap-2"
        >
          <Button
            nativeButton={false}
            role="link"
            render={<a href={`${repository}/releases`} />}
            variant="ghost"
          >
            Releases
            <ArrowUpRight />
          </Button>
          <Button
            nativeButton={false}
            role="link"
            render={<a href={repository} />}
            variant="outline"
          >
            GitHub
            <ArrowUpRight />
          </Button>
          <ThemeToggle iconOnly />
        </nav>
      </header>
      <main id="main" tabIndex={-1}>
        <div className="site-hero grid items-center gap-10 py-14 lg:grid-cols-[1.05fr_1fr] lg:gap-12 lg:py-24">
          <section className="min-w-0">
            <p className="mb-5 flex items-center gap-2 text-sm font-medium text-primary">
              <Server className="size-4" />
              Self-hosted. On your terms.
            </p>
            <h1 className="text-[clamp(2.7rem,5vw,4.4rem)] font-semibold leading-[1.06] tracking-[-0.045em]">
              Your projects.
              <br />
              Your machine.
              <br />
              <span className="text-primary">One Groundplane.</span>
            </h1>
            <p className="mt-6 max-w-md text-lg leading-relaxed text-muted-foreground">
              A control plane for the applications you run. Bring your projects
              together on one host.
            </p>
            <Card className="mt-8">
              <CardHeader className="flex-row items-center justify-between gap-2 py-4">
                <h2 className="flex items-center gap-2 text-sm font-semibold">
                  <Terminal className="size-4 text-primary" />
                  Install Groundplane
                </h2>
                <span className="text-xs text-muted-foreground">Linux</span>
              </CardHeader>
              <CardContent className="space-y-4">
                <pre
                  tabIndex={0}
                  aria-label="Installation command"
                  className="whitespace-pre-wrap break-all rounded-lg border border-input bg-background p-3 font-mono text-xs leading-7"
                >
                  <code>{command}</code>
                </pre>
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <Button
                    nativeButton={false}
                    role="link"
                    render={<a href="/install.sh" />}
                    variant="link"
                    size="sm"
                  >
                    Review installer
                    <ArrowUpRight />
                  </Button>
                  <CopyButton
                    value={command}
                    label="Copy command"
                    className="border border-border bg-surface px-3 py-2"
                  />
                </div>
              </CardContent>
            </Card>
            <p className="mt-4 text-xs leading-6 text-muted-foreground">
              Ubuntu 24.04 / 26.04 · Debian 13
              <br />
              amd64 / arm64
            </p>
          </section>
          <Architecture />
        </div>
        <section
          aria-label="Why Groundplane"
          className="grid gap-4 border-t border-border py-8 md:grid-cols-3"
        >
          {[
            {
              icon: Boxes,
              title: "One place to operate",
              text: "Console, CLI and API. Choose your interface.",
            },
            {
              icon: Server,
              title: "Your infrastructure",
              text: "Run on a machine you control.",
            },
            {
              icon: Terminal,
              title: "Start with one command",
              text: "The installer selects the latest stable release.",
            },
          ].map(({ icon: Icon, title, text }) => (
            <Card key={title}>
              <CardContent>
                <Icon className="mb-4 size-5 text-primary" />
                <h2 className="text-base font-semibold">{title}</h2>
                <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                  {text}
                </p>
              </CardContent>
            </Card>
          ))}
        </section>
      </main>
      <footer className="flex flex-wrap items-center justify-between gap-4 border-t border-border py-7 text-xs text-muted-foreground">
        <span>Groundplane · Self-hosted control plane</span>
        <div className="flex flex-wrap gap-5">
          <a href={`${repository}/blob/main/docs/deployment.md`}>
            Installation guide
            <ArrowUpRight className="ml-1 inline size-3" />
          </a>
          <a href="/install.sh.sha256">
            Installer checksum
            <ArrowUpRight className="ml-1 inline size-3" />
          </a>
        </div>
      </footer>
    </div>
  );
}

function Architecture() {
  return (
    <figure
      aria-label="Illustrative architecture, not live host status"
      className="site-diagram min-w-0 rounded-xl border border-border bg-card p-5 sm:p-7"
    >
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span className="flex items-center gap-2">
          <Workflow className="size-4" />
          One host, many applications
        </span>
        <span>Illustration</span>
      </div>
      <div className="mt-8 grid grid-cols-3 gap-3">
        {["Console", "CLI", "API"].map((label) => (
          <div
            key={label}
            className="rounded-lg border border-border bg-background py-3 text-center text-sm font-medium"
          >
            {label}
          </div>
        ))}
      </div>
      <div className="site-join" aria-hidden="true">
        <i />
        <i />
        <i />
      </div>
      <div className="rounded-xl border border-primary/30 bg-primary/10 p-5 text-center">
        <Layers
          className="mx-auto mb-2 size-6 text-primary"
          strokeWidth={1.6}
        />
        <span className="text-xs uppercase tracking-widest text-muted-foreground">
          Groundplane
        </span>
        <h2 className="mt-1 text-2xl font-semibold">Controller</h2>
      </div>
      <div className="site-wire" aria-hidden="true" />
      <div className="mx-auto flex w-fit items-center gap-2 rounded-lg border border-border bg-background px-6 py-3 text-sm font-medium">
        <Server className="size-4 text-primary" />
        Agent
      </div>
      <div className="site-join site-join-reverse" aria-hidden="true">
        <i />
        <i />
        <i />
      </div>
      <div className="grid grid-cols-3 gap-3">
        {[
          { icon: Globe, label: "Web app" },
          { icon: Code2, label: "API service" },
          { icon: Workflow, label: "Worker" },
        ].map(({ icon: Icon, label }) => (
          <div
            key={label}
            className="min-w-0 rounded-lg border border-border bg-background px-1 py-4 text-center text-xs"
          >
            <Icon className="mx-auto mb-2 size-4 text-primary" />
            {label}
          </div>
        ))}
      </div>
      <figcaption className="mt-7 text-center text-xs text-muted-foreground">
        Many projects. One place to manage them.
      </figcaption>
    </figure>
  );
}
