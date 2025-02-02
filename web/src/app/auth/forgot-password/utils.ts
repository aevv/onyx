export const forgotPassword = async (email: string): Promise<void> => {
  const response = await fetch(`/api/auth/forgot-password`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ email }),
  });

  if (!response.ok) {
    const error = await response.json();
    const errorMessage =
      error?.detail || "An error occurred during password reset.";
    throw new Error(errorMessage);
  }
};

export const resetPassword = async (
  token: string,
  password: string
): Promise<Response> => {
  return new Response(null, { status: 400 });
};
