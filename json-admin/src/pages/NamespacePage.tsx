import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/api";
import { Header } from "../components/Header";

export function NamespacesPage() {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ["namespaces"],
    queryFn: api.listNamespaces,
  });

  return (
    <>
      <Header />
      <main className="mx-auto max-w-6xl px-4 py-6">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold">Namespaces</h1>
          <button onClick={() => refetch()} className="px-3 py-2 border rounded">
            Refresh
          </button>
        </div>

        {isLoading && <p className="mt-4">Загрузка…</p>}
        {isError && <p className="mt-4 text-red-600">Ошибка: {(error as Error).message}</p>}

        {data && (
          <ul className="mt-4 space-y-2">
            {data.map((ns) => (
              <li key={ns} className="p-3 border rounded">
                <div className="font-mono">{ns}</div>
              </li>
            ))}
          </ul>
        )}
      </main>
    </>
  );
}