import { Header } from "../components/Header";

export function HomePage() {
  return (
    <>
      <Header />
      <main className="mx-auto max-w-6xl px-4 py-6">
        <h1 className="text-2xl font-semibold mb-2">Добро ать</h1>
        <p className="text-gray-600">
          Это админ-панель для хранения и поиска JSON-ов по namespace.
        </p>
      </main>
    </>
  );
}