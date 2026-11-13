import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";

type Props = {
  autoUpdate: boolean;
  setAutoUpdate: (value: boolean) => void;
  githubName: string;
  setGithubName: (value: string) => void;
  runnerGroup: string;
  setRunnerGroup: (value: string) => void;
  labels: string;
  setLabels: (value: string) => void;
  registrationToken: string;
  setRegistrationToken: (value: string) => void;
  cpus: string;
  setCpus: (value: string) => void;
  memoryGiB: string;
  setMemoryGiB: (value: string) => void;
  submittingCreate: boolean;
  createError: string | null;
};

export function RunnerCreationFields({
  autoUpdate,
  setAutoUpdate,
  githubName,
  runnerGroup,
  labels,
  registrationToken,
  cpus,
  memoryGiB,
  setGithubName,
  setRunnerGroup,
  setLabels,
  setRegistrationToken,
  setCpus,
  setMemoryGiB,
  submittingCreate,
  createError,
}: Props) {
  return (
    <>
      <fieldset className="grid gap-4 md:grid-cols-2 rounded-lg border border-border p-4">
        <legend className="px-2 text-xs font-medium">Registration</legend>
        <div className="flex items-center gap-2 md:col-span-2">
          <Checkbox
            id="runner-create-auto-update"
            checked={autoUpdate}
            onChange={(event) => setAutoUpdate(event.target.checked)}
            disabled={submittingCreate}
          />
          <Label htmlFor="runner-create-auto-update">
            Automatically update GitHub runner software
          </Label>
        </div>
        <div className="space-y-2">
          <Label htmlFor="runner-create-name">
            GitHub Runner name (optional)
          </Label>
          <Input
            id="runner-create-name"
            value={githubName}
            onChange={(event) => setGithubName(event.target.value)}
            maxLength={64}
            disabled={submittingCreate}
            placeholder="Generated automatically"
          />
          <p className="text-xs text-muted-foreground">
            Letters, digits, dots, underscores or hyphens. Independent of the GP
            slug.
          </p>
        </div>
        <div className="space-y-2">
          <Label htmlFor="runner-create-group">GitHub Runner group</Label>
          <Input
            id="runner-create-group"
            value={runnerGroup}
            onChange={(event) => setRunnerGroup(event.target.value)}
            maxLength={255}
            disabled={submittingCreate}
            required
          />
          <p className="text-xs text-muted-foreground">
            Existing group name for an organization URL. Repository Runners use
            Default. Manage group access in GitHub.
          </p>
        </div>
        <div className="space-y-2">
          <Label htmlFor="runner-create-labels">Labels</Label>
          <Input
            id="runner-create-labels"
            value={labels}
            onChange={(event) => setLabels(event.target.value)}
            placeholder="build, deployment"
          />
          <p className="text-xs text-muted-foreground">
            Optional, comma-separated. Groundplane adds the standard GitHub
            labels.
          </p>
        </div>
        <div className="space-y-2">
          <Label htmlFor="runner-create-token">Registration token</Label>
          <Input
            id="runner-create-token"
            type="password"
            autoComplete="off"
            value={registrationToken}
            onChange={(event) => setRegistrationToken(event.target.value)}
          />
          {createError && (
            <p className="text-sm text-destructive">{createError}</p>
          )}
        </div>
      </fieldset>
      <fieldset className="grid gap-4 rounded-lg border border-border p-4 md:grid-cols-2">
        <legend className="px-2 text-xs font-medium">Resource budget</legend>
        <div className="space-y-2">
          <Label htmlFor="runner-create-cpus">CPUs</Label>
          <Input
            id="runner-create-cpus"
            type="number"
            min="0.1"
            max="1024"
            step="0.1"
            value={cpus}
            onChange={(event) => setCpus(event.target.value)}
            required
            disabled={submittingCreate}
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="runner-create-memory">Memory (GiB)</Label>
          <Input
            id="runner-create-memory"
            type="number"
            min="0.5"
            max="1024"
            step="0.5"
            value={memoryGiB}
            onChange={(event) => setMemoryGiB(event.target.value)}
            required
            disabled={submittingCreate}
          />
        </div>
        <p className="text-xs text-muted-foreground md:col-span-2">
          Combined limit for this Runner, its Docker daemon and build processes.
          These limits do not reserve host capacity.
        </p>
      </fieldset>
    </>
  );
}
